// 동기화된 청킹 워커 - SharedArrayBuffer 공유 큐 사용
import { jbfilereader } from './jbfilereader.js';

class SynchronizedChunkingWorker {
    constructor() {
        // 일반 배열 기반 큐 (SharedArrayBuffer 대안)
        this.chunkQueue = [];
        this.queueSize = 1000; // 최대 큐 크기
        this.isQueueReady = false;
    }
    
    // 일반 큐 초기화
    initializeQueue() {
        this.chunkQueue = [];
        this.isQueueReady = true;
        console.log('📋 청크 큐 초기화 완료 (일반 배열 방식)');
    }
    
    // 청크 위치 정보를 큐에 추가
    enqueueChunk(chunkInfo) {
        if (this.chunkQueue.length >= this.queueSize) {
            console.warn(`⚠️ 큐 가득참, 대기 중... (${this.chunkQueue.length}/${this.queueSize})`);
            return false; // 큐가 가득참
        }
        
        // 청크 정보를 큐에 저장
        this.chunkQueue.push(chunkInfo);
        
        console.log(`📋 청크 ${chunkInfo.chunkId} 큐에 추가 (큐 크기: ${this.chunkQueue.length}/${this.queueSize})`);
        
        // 즉시 워커들에게 청크 전송
        self.postMessage({
            type: 'chunk_available',
            chunkInfo: chunkInfo
        });
        
        return true;
    }
    
    async chunkBothFiles(file1, file2, chunkSize = 10000) {
        const reader1 = new jbfilereader(file1, false);
        const reader2 = new jbfilereader(file2, false);
        let recordCount = 0;
        let chunkId = 0;
        
        this.initializeQueue();
        
        // 큐 준비 완료를 워커들에게 알림
        self.postMessage({
            type: 'queue_ready'
        });
        
        while (true) {
            let file1ChunkPosition = reader1.fpos; // FASTQ1 청크 시작 위치
            let file2ChunkPosition = reader2.fpos; // FASTQ2 청크 시작 위치
            let chunkRecordCount = 0;
            
            // 🔒 핵심: 두 파일에서 동시에 정확히 같은 개수의 레코드 위치 확인
            // paired-read 동기화를 위해 반드시 같은 레코드 수로 청킹
            for (let i = 0; i < chunkSize; i++) {
                // 파일1에서 1개 레코드 건너뛰기 (위치만 기록)
                const record1Exists = await this.skipSingleRecord(reader1);
                if (!record1Exists) break; // EOF
                
                // 파일2에서 1개 레코드 건너뛰기 (위치만 기록)
                const record2Exists = await this.skipSingleRecord(reader2);
                if (!record2Exists) {
                    throw new Error('⚠️ FASTQ1과 FASTQ2의 레코드 수가 일치하지 않습니다! paired-read 동기화 실패');
                }
                
                chunkRecordCount++;
                recordCount++;
            }
            
            if (chunkRecordCount === 0) break; // EOF 도달
            
            // ✅ 중요: 두 파일 모두 정확히 같은 레코드 수로 청킹 완료된 경우에만 큐에 추가
            // 이렇게 해야 워커에서 i번째 FASTQ1 레코드와 i번째 FASTQ2 레코드가 정확히 매칭됨
            const chunkPositionInfo = {
                chunkId,
                file1StartPos: file1ChunkPosition,
                file2StartPos: file2ChunkPosition,
                recordCount: chunkRecordCount, // 두 파일 모두 동일한 레코드 수 보장
                recordStart: recordCount - chunkRecordCount,
                recordEnd: recordCount - 1,
                // paired-read 동기화 검증 정보
                syncValidated: true,
                timestamp: Date.now()
            };
            
            console.log(`🔒 청크 ${chunkId}: FASTQ1과 FASTQ2 모두 ${chunkRecordCount}개 레코드로 동기화 완료`);
            console.log(`   📍 FASTQ1 위치: ${file1ChunkPosition} → ${reader1.fpos}`);
            console.log(`   📍 FASTQ2 위치: ${file2ChunkPosition} → ${reader2.fpos}`);
            
            // 큐가 가득 찰 때까지 대기
            while (!this.enqueueChunk(chunkPositionInfo)) {
                await new Promise(resolve => setTimeout(resolve, 10));
            }
            
            chunkId++;
        }
        
        // 청킹 완료 신호
        self.postMessage({
            type: 'chunking_complete',
            totalChunks: chunkId,
            totalRecords: recordCount
        });
    }
    
    async skipSingleRecord(reader) {
        // FASTQ 레코드 4줄 건너뛰기
        const id = await this.readLine(reader);
        if (id === "") return false; // EOF
        
        await this.readLine(reader); // seq
        await this.readLine(reader); // plus
        await this.readLine(reader); // qual
        
        return true;
    }
    
    async readLine(reader) {
        return new Promise((resolve) => {
            reader.readline(resolve);
        });
    }
}

// 워커 메인 로직
const chunkingWorker = new SynchronizedChunkingWorker();

self.addEventListener('message', async function(event) {
    const { type, file1, file2, chunkSize } = event.data;
    
    if (type === 'start_chunking') {
        try {
            console.log('🚀 동기화된 청킹 시작');
            await chunkingWorker.chunkBothFiles(file1, file2, chunkSize);
            console.log('✅ 청킹 완료');
        } catch (error) {
            console.error('❌ 청킹 오류:', error);
            self.postMessage({
                type: 'chunking_error',
                error: error.message
            });
        }
    }
});