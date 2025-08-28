// DuckDB 통합 - 중복 제거 및 카운팅
import * as duckdb from '@duckdb/duckdb-wasm';

export class DuckDBProcessor {
    constructor() {
        this.db = null;
        this.conn = null;
        this.initialized = false;
    }

    // DuckDB 초기화
    async initialize() {
        if (this.initialized) return;

        try {
            // DuckDB 번들 로드
            const JSDELIVR_BUNDLES = duckdb.getJsDelivrBundles();
            const bundle = await duckdb.selectBundle(JSDELIVR_BUNDLES);
            
            const worker_url = URL.createObjectURL(
                new Blob([`importScripts("${bundle.mainWorker}");`], { type: 'text/javascript' })
            );

            const worker = new Worker(worker_url);
            const logger = new duckdb.ConsoleLogger();
            
            this.db = new duckdb.AsyncDuckDB(logger, worker);
            await this.db.instantiate(bundle.mainModule, bundle.pthreadWorker);
            
            this.conn = await this.db.connect();
            
            // 테이블 초기화
            await this.createTables();
            
            this.initialized = true;
            console.log('DuckDB 초기화 완료');
            
        } catch (error) {
            console.error('DuckDB 초기화 실패:', error);
            throw error;
        }
    }

    // 필요한 테이블 생성
    async createTables() {
        // 선택된 시퀀스 테이블
        await this.conn.query(`
            CREATE TABLE IF NOT EXISTS selected_sequences (
                id INTEGER PRIMARY KEY,
                sequence VARCHAR,
                quality VARCHAR,
                worker_id INTEGER,
                chunk_id INTEGER
            )
        `);

        // 첫 번째 정렬 결과 테이블
        await this.conn.query(`
            CREATE TABLE IF NOT EXISTS sorted1_sequences (
                id INTEGER PRIMARY KEY,
                sequence VARCHAR,
                quality VARCHAR,
                worker_id INTEGER,
                chunk_id INTEGER
            )
        `);

        // 두 번째 정렬 결과 테이블
        await this.conn.query(`
            CREATE TABLE IF NOT EXISTS sorted2_sequences (
                id INTEGER PRIMARY KEY,
                sequence VARCHAR,
                quality VARCHAR,
                worker_id INTEGER,
                chunk_id INTEGER
            )
        `);

        // 최종 카운트 결과 테이블
        await this.conn.query(`
            CREATE TABLE IF NOT EXISTS sequence_counts (
                sequence VARCHAR PRIMARY KEY,
                count INTEGER,
                avg_quality DOUBLE
            )
        `);

        console.log('DuckDB 테이블 생성 완료');
    }

    // 시퀀스 데이터 삽입
    async insertSequences(tableName, sequences, workerId, chunkId) {
        if (!this.initialized) {
            await this.initialize();
        }

        if (!sequences || sequences.length === 0) return;

        try {
            // 배치 삽입을 위한 준비
            const values = sequences.map((seq, index) => {
                const sequence = seq.seq || seq;
                const quality = seq.qual || '';
                const id = chunkId * 10000 + index; // 유니크 ID 생성
                
                return `(${id}, '${sequence.replace(/'/g, "''")}', '${quality.replace(/'/g, "''")}', ${workerId}, ${chunkId})`;
            }).join(',');

            const query = `
                INSERT INTO ${tableName} (id, sequence, quality, worker_id, chunk_id)
                VALUES ${values}
            `;

            await this.conn.query(query);
            console.log(`${tableName}에 ${sequences.length}개 시퀀스 삽입 완료`);

        } catch (error) {
            console.error(`시퀀스 삽입 오류 (${tableName}):`, error);
            throw error;
        }
    }

    // 중복 제거 및 카운팅
    async deduplicateAndCount(tableName) {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            // 중복 제거하고 카운트 계산
            const query = `
                SELECT 
                    sequence,
                    COUNT(*) as count,
                    AVG(LENGTH(quality)) as avg_quality_length
                FROM ${tableName}
                GROUP BY sequence
                ORDER BY count DESC, sequence
                LIMIT 10
            `;

            const result = await this.conn.query(query);
            const rows = result.toArray();
            
            console.log(`${tableName} 중복 제거 완료: ${rows.length}개 고유 시퀀스`);
            
            return rows.map(row => ({
                sequence: row.sequence,
                count: row.count,
                avgQualityLength: row.avg_quality_length
            }));

        } catch (error) {
            console.error(`중복 제거 오류 (${tableName}):`, error);
            throw error;
        }
    }

    // 전체 카운트 조회
    async getTotalCounts() {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const results = {};
            
            // 각 테이블의 총 레코드 수 조회
            const tables = ['selected_sequences', 'sorted1_sequences', 'sorted2_sequences'];
            
            for (const table of tables) {
                const result = await this.conn.query(`SELECT COUNT(*) as total FROM ${table}`);
                const rows = result.toArray();
                results[table] = rows[0]?.total || 0;
            }
            
            return {
                selected: results.selected_sequences,
                sorted1: results.sorted1_sequences,
                sorted2: results.sorted2_sequences
            };

        } catch (error) {
            console.error('전체 카운트 조회 오류:', error);
            throw error;
        }
    }

    // 워커별 카운트 조회
    async getWorkerCounts(workerId) {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const results = {};
            const tables = ['selected_sequences', 'sorted1_sequences', 'sorted2_sequences'];
            
            for (const table of tables) {
                const result = await this.conn.query(`
                    SELECT COUNT(*) as total 
                    FROM ${table} 
                    WHERE worker_id = ${workerId}
                `);
                const rows = result.toArray();
                results[table] = rows[0]?.total || 0;
            }
            
            return {
                selected: results.selected_sequences,
                sorted1: results.sorted1_sequences,
                sorted2: results.sorted2_sequences
            };

        } catch (error) {
            console.error(`워커 ${workerId} 카운트 조회 오류:`, error);
            throw error;
        }
    }

    // 상위 N개 시퀀스 조회
    async getTopSequences(tableName, limit = 10) {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const query = `
                SELECT 
                    sequence,
                    COUNT(*) as count
                FROM ${tableName}
                GROUP BY sequence
                ORDER BY count DESC
                LIMIT ${limit}
            `;

            const result = await this.conn.query(query);
            return result.toArray();

        } catch (error) {
            console.error(`상위 시퀀스 조회 오류 (${tableName}):`, error);
            throw error;
        }
    }

    // 전체 시퀀스와 카운트 조회 (다운로드용)
    async getAllSequencesWithCounts(tableName) {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const query = `
                SELECT 
                    sequence,
                    COUNT(*) as count
                FROM ${tableName}
                GROUP BY sequence
                ORDER BY count DESC, sequence
            `;

            const result = await this.conn.query(query);
            return result.toArray();

        } catch (error) {
            console.error(`전체 시퀀스 조회 오류 (${tableName}):`, error);
            throw error;
        }
    }

    // 테이블 초기화
    async clearTables() {
        if (!this.initialized) {
            await this.initialize();
        }

        try {
            const tables = ['selected_sequences', 'sorted1_sequences', 'sorted2_sequences', 'sequence_counts'];
            
            for (const table of tables) {
                await this.conn.query(`DELETE FROM ${table}`);
            }
            
            console.log('모든 테이블 초기화 완료');

        } catch (error) {
            console.error('테이블 초기화 오류:', error);
            throw error;
        }
    }

    // 데이터베이스 종료
    async close() {
        if (this.conn) {
            await this.conn.close();
        }
        if (this.db) {
            await this.db.terminate();
        }
        this.initialized = false;
        console.log('DuckDB 연결 종료');
    }
}