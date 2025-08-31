// Pull 기반 처리 워커 - 큐에서 자율적으로 작업을 가져와서 처리
import { jbfilereader } from './jbfilereader.js';
import { run_fastq_join } from './fastq-join.js';

class PullBasedProcessingWorker {
    constructor() {
        this.workerId = null;
        this.isProcessing = false;
        this.totalProcessedChunks = 0;
        this.chunkingComplete = false;
        this.totalChunks = 0;
        this.files = null;
        this.analysisParams = null;
        this.isIdle = true;
        this.queueEmpty = true;
        this.heartbeatInterval = null;
    }
    
    // 하트비트 시작
    startHeartbeat() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
        }
        
        this.heartbeatInterval = setInterval(() => {
            self.postMessage({
                type: 'worker_heartbeat',
                workerId: this.workerId,
                isIdle: this.isIdle,
                processedChunks: this.totalProcessedChunks,
                queueEmpty: this.queueEmpty,
                chunkingComplete: this.chunkingComplete,
                timestamp: Date.now()
            });
        }, 5000); // 5초마다 하트비트 전송
        
        console.log(`💓 워커 ${this.workerId}: 하트비트 시작 (5초 간격)`);
    }
    
    // 하트비트 중지
    stopHeartbeat() {
        if (this.heartbeatInterval) {
            clearInterval(this.heartbeatInterval);
            this.heartbeatInterval = null;
            console.log(`💓 워커 ${this.workerId}: 하트비트 중지`);
        }
    }
    
    // 처리 루프 시작
    async startProcessingLoop(files, analysisParams) {
        this.files = files;
        this.analysisParams = analysisParams;
        
        console.log(`🔄 워커 ${this.workerId}: 처리 루프 시작`);
        
        // 하트비트 시작
        this.startHeartbeat();
        
        while (true) {
            // 큐에서 청크 가져오기
            const chunkInfo = this.dequeueChunk();
            
            if (chunkInfo) {
                // 청크 처리 시작 - 바쁜 상태로 변경
                this.isIdle = false;
                this.queueEmpty = false;
                
                await this.processChunkFromQueue(chunkInfo);
                this.totalProcessedChunks++;
                
                // 청크 처리 완료 - 유휴 상태로 변경
                this.isIdle = true;
                
            } else {
                // 큐가 비어있음
                this.queueEmpty = true;
                this.isIdle = true;
                if (this.chunkingComplete && this.totalProcessedChunks >= this.totalChunks) {
                    // 모든 청크 처리 완료
                    console.log(`✅ 워커 ${this.workerId}: 모든 청크 처리 완료`);
                    this.stopHeartbeat(); // 하트비트 중지
                    self.postMessage({
                        type: 'all_chunks_processed',
                        workerId: this.workerId,
                        totalProcessed: this.totalProcessedChunks
                    });
                    break;
                    
                } else {
                    // 잠시 대기 후 다시 시도
                    self.postMessage({
                        type: 'worker_idle',
                        workerId: this.workerId
                    });
                    
                    await new Promise(resolve => setTimeout(resolve, 50)); // 50ms 대기
                    continue;
                }
            }
        }
    }
    
    // 큐에서 가져온 청크 처리
    async processChunkFromQueue(chunkInfo) {
        console.log(`🔧 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 처리 시작`);
        
        try {
            // 동기화 검증 확인
            if (!chunkInfo.syncValidated) {
                throw new Error(`청크 ${chunkInfo.chunkId}: paired-read 동기화 검증 실패`);
            }
            
            console.log(`🔒 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} paired-read 동기화 확인됨 (${chunkInfo.recordCount}개 레코드)`);
            
            // 1. 파일에서 해당 위치의 데이터 읽기 (정확히 같은 레코드 수)
            const [file1Records, file2Records] = await Promise.all([
                this.readChunkDataFromPosition(this.files[0], chunkInfo.file1StartPos, chunkInfo.recordCount),
                this.readChunkDataFromPosition(this.files[1], chunkInfo.file2StartPos, chunkInfo.recordCount)
            ]);
            
            // 추가 안전성 검증: 읽어온 레코드 수가 동일한지 확인
            if (file1Records.length !== file2Records.length) {
                throw new Error(`청크 ${chunkInfo.chunkId}: 읽어온 레코드 수 불일치 (FASTQ1: ${file1Records.length}, FASTQ2: ${file2Records.length})`);
            }
            
            console.log(`📖 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 데이터 읽기 완료 (FASTQ1: ${file1Records.length}, FASTQ2: ${file2Records.length})`);
            
            // 2. Paired-Read 조인 (CLAUDE.md 가이드라인: run_fastq_join 사용)
            console.log(`🔗 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} paired-read 조인 시작 (run_fastq_join 사용)`);
            
            // Paired-read 검증 수행
            let validationErrors = 0;
            for (let i = 0; i < file1Records.length; i++) {
                try {
                    this.validatePairedReads(file1Records[i], file2Records[i], i);
                } catch (error) {
                    console.error(`워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 레코드 ${i} 검증 실패:`, error.message);
                    validationErrors++;
                }
            }
            
            // run_fastq_join으로 조인 수행
            const joinedSequences = await this.runFastqJoinOnChunk(
                file1Records, 
                file2Records, 
                this.analysisParams.is_short
            );
            
            if (validationErrors > 0) {
                console.warn(`⚠️ 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId}에서 ${validationErrors}개 레코드 검증 실패`);
            }
            
            console.log(`✅ 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} paired-read 조인 완료 (${joinedSequences.length}개 조인 시퀀스, 검증 오류: ${validationErrors}개)`);
            
            // 3. run_fastq_join 결과를 processChunk에 맞는 형태로 변환
            const processableSequences = joinedSequences.map((seq, index) => ({
                seq: seq,
                qual: '', // run_fastq_join은 quality score를 반환하지 않음
                id: `joined_${index}`,
                validated: true,
                recordIndex: index
            }));
            
            // 4. 3단계 필터링 로직 실행
            const result = await this.processChunk({
                joinedSequences: processableSequences,
                selectionParams: {
                    read1: this.analysisParams.sel_read1,
                    read2: this.analysisParams.sel_read2
                },
                sort1Params: {
                    read1: this.analysisParams.s1_read1,
                    read2: this.analysisParams.s1_read2,
                    length: this.analysisParams.s1_l
                },
                sort2Params: {
                    read1: this.analysisParams.s2_read1,
                    read2: this.analysisParams.s2_read2,
                    length: this.analysisParams.s2_l
                },
                chunkId: chunkInfo.chunkId
            });
            
            // 검증 통계 추가
            result.validationStats = {
                totalRecords: file1Records.length,
                validRecords: joinedSequences.length,
                errorRecords: validationErrors,
                errorRate: (validationErrors / file1Records.length * 100).toFixed(2) + '%'
            };
            
            // 4. 결과를 메인 스레드로 전송
            self.postMessage({
                type: 'chunk_processed',
                workerId: this.workerId,
                chunkId: chunkInfo.chunkId,
                data: result
            });
            
            console.log(`✅ 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 처리 완료`);
            
        } catch (error) {
            console.error(`❌ 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 처리 오류:`, error);
            self.postMessage({
                type: 'chunk_error',
                workerId: this.workerId,
                chunkId: chunkInfo.chunkId,
                error: error.message
            });
        }
    }
    
    // 위치에서 청크 데이터 읽기
    async readChunkDataFromPosition(file, startPos, recordCount) {
        console.log(`📖 워커 ${this.workerId}: ${file.name}에서 위치 ${startPos}부터 ${recordCount}개 레코드 읽기`);
        
        const reader = new jbfilereader(file, false);
        const records = [];
        
        // 시작 위치로 이동 (간단한 방법: 처음부터 읽어서 건너뛰기)
        let currentRecords = 0;
        let targetStartRecord = Math.floor(startPos / 200); // 대략적인 레코드 위치 추정
        
        // 첫 번째 청크가 아니면 해당 위치까지 건너뛰기
        if (targetStartRecord > 0) {
            for (let i = 0; i < targetStartRecord; i++) {
                await this.readLine(reader); // id
                await this.readLine(reader); // seq
                await this.readLine(reader); // plus
                await this.readLine(reader); // qual
            }
        }
        
        // 실제 레코드 읽기
        for (let i = 0; i < recordCount; i++) {
            const record = {
                id: await this.readLine(reader),
                seq: await this.readLine(reader),
                plus: await this.readLine(reader),
                qual: await this.readLine(reader)
            };
            
            if (record.id === "" || !record.id.startsWith('@')) {
                break;
            }
            
            records.push(record);
        }
        
        return records;
    }
    
    async readLine(reader) {
        return new Promise((resolve) => {
            reader.readline(resolve);
        });
    }
    
    // Paired-read 동기화 검증 함수
    validatePairedReads(record1, record2, recordIndex) {
        // 1. 레코드 존재 확인
        if (!record1 || !record2) {
            throw new Error(`레코드 ${recordIndex}: paired-read 중 하나가 누락됨 (FASTQ1: ${!!record1}, FASTQ2: ${!!record2})`);
        }
        
        // 2. 기본 구조 검증
        if (!record1.id || !record1.seq || !record1.qual || !record2.id || !record2.seq || !record2.qual) {
            throw new Error(`레코드 ${recordIndex}: FASTQ 레코드 구조가 불완전함`);
        }
        
        // // 3. ID 기반 paired-read 매칭 검증 (선택적)
        // const id1Base = record1.id.split(' ')[0].replace(/\/[12]$/, ''); // @read_id 부분에서 /1, /2 제거
        // const id2Base = record2.id.split(' ')[0].replace(/\/[12]$/, '');
        
        // if (id1Base !== id2Base) {
        //     console.warn(`레코드 ${recordIndex}: Paired-read ID 불일치 감지 (${id1Base} vs ${id2Base})`);
        //     // 경고만 출력하고 처리 계속 (일부 FASTQ 파일은 ID 형식이 다를 수 있음)
        // }
        
        // // 4. 시퀀스 길이 검증 (비정상적으로 짧거나 긴 시퀀스 감지)
        // if (record1.seq.length < 10 || record2.seq.length < 10) {
        //     console.warn(`레코드 ${recordIndex}: 비정상적으로 짧은 시퀀스 (FASTQ1: ${record1.seq.length}bp, FASTQ2: ${record2.seq.length}bp)`);
        // }
        
        // 5. Quality 점수와 시퀀스 길이 일치 확인
        if (record1.seq.length !== record1.qual.length || record2.seq.length !== record2.qual.length) {
            throw new Error(`레코드 ${recordIndex}: 시퀀스와 품질 점수 길이 불일치`);
        }
        
        return true;
    }
    
    // CLAUDE.md 가이드라인: run_fastq_join 함수 사용하는 청크 단위 조인
    async runFastqJoinOnChunk(file1Data, file2Data, is_short) {
        // 1. FASTQ 레코드 배열을 FASTQ 문자열로 변환
        const file1String = this.convertRecordsToFastqString(file1Data);
        const file2String = this.convertRecordsToFastqString(file2Data);
        
        // 2. Blob으로 변환하여 File 객체 생성
        const file1Blob = new Blob([file1String], {type: 'text/plain'});
        const file2Blob = new Blob([file2String], {type: 'text/plain'});
        const tempFiles = [
            new File([file1Blob], 'temp1.fq'), 
            new File([file2Blob], 'temp2.fq')
        ];
        
        // 3. run_fastq_join 호출
        return new Promise((resolve, reject) => {
            const joinedSequences = [];
            
            const pgCallback = (progress) => {
                // 청크 단위에서는 진행률 콜백 무시
            };
            
            const chunkCallback = (joins) => {
                // join 결과를 수집
                joinedSequences.push(...joins);
            };
            
            try {
                console.log(`🔗 워커 ${this.workerId}: run_fastq_join 실행 시작 (${file1Data.length}개 레코드, is_short: ${is_short})`);
                
                // run_fastq_join 매개변수: (files, pgcallback, chunkcallback, reverse, mino, pctdiff)
                // is_short=true면 reverse=false (순서 그대로), is_short=false면 reverse=true (역순)
                const reverse = !is_short;
                run_fastq_join(tempFiles, pgCallback, chunkCallback, reverse, 6, 8);
                
                console.log(`✅ 워커 ${this.workerId}: run_fastq_join 완료 (${joinedSequences.length}개 조인 시퀀스)`);
                resolve(joinedSequences);
                
            } catch (error) {
                console.error(`❌ 워커 ${this.workerId}: run_fastq_join 오류:`, error);
                reject(error);
            }
        });
    }
    
    // FASTQ 레코드 배열을 FASTQ 문자열로 변환하는 헬퍼 함수
    convertRecordsToFastqString(records) {
        return records.map(record => 
            `${record.id}\n${record.seq}\n${record.comment || '+'}\n${record.qual}`
        ).join('\n') + '\n';
    }
    
    
    // 미스매치 개수 세기
    countMismatches(seq1, start1, seq2, start2, length) {
        let mismatches = 0;
        for (let i = 0; i < length; i++) {
            if (seq1[start1 + i] !== seq2[start2 + i]) {
                mismatches++;
            }
        }
        return mismatches;
    }

    // 시퀀스 필터링 함수
    filterSequences(sequences, pattern1, pattern2, maxMismatches = 1, requiredLength = 0, onlyBetween = false) {
        const results = [];
        const p1Len = pattern1.length;
        const p2Len = pattern2.length;

        for (const seq of sequences) {
            const seqStr = seq.seq || seq;
            const seqLen = seqStr.length;
            let found = false;

            outerLoop:
            for (let i = 0; i <= seqLen - p1Len; i++) {
                if (this.matchesWithMismatches(seqStr, pattern1, i, maxMismatches)) {
                    for (let j = i + p1Len; j <= seqLen - p2Len; j++) {
                        if (this.matchesWithMismatches(seqStr, pattern2, j, maxMismatches)) {
                            const betweenLength = j - (i + p1Len);
                            if (requiredLength === 0 || betweenLength === requiredLength) {
                                if (onlyBetween) {
                                    results.push({
                                        seq: seqStr.slice(i, j + p2Len),
                                        qual: seq.qual ? seq.qual.slice(i, j + p2Len) : '',
                                        id: seq.id || '',
                                        original: seq
                                    });
                                } else {
                                    results.push(seq);
                                }
                                found = true;
                                break outerLoop;
                            }
                        }
                    }
                }
            }
        }

        return results;
    }

    // 미스매치를 허용하여 패턴 매칭
    matchesWithMismatches(sequence, pattern, startPos, maxMismatches) {
        let mismatches = 0;
        for (let i = 0; i < pattern.length; i++) {
            if (sequence[startPos + i] !== pattern[i]) {
                mismatches++;
                if (mismatches > maxMismatches) {
                    return false;
                }
            }
        }
        return true;
    }

    // 중복 제거 및 카운팅
    countUniqueSequences(sequences) {
        const countMap = new Map();
        
        for (const sequence of sequences) {
            const seq = sequence.seq || sequence;
            if (countMap.has(seq)) {
                countMap.set(seq, countMap.get(seq) + 1);
            } else {
                countMap.set(seq, 1);
            }
        }
        
        // 카운트 순으로 정렬하여 배열로 변환
        return Array.from(countMap.entries())
            .map(([sequence, count]) => ({ sequence, count }))
            .sort((a, b) => b.count - a.count);
    }

    
    // 위치에서 청크 데이터 읽기
    async readChunkDataFromPosition(file, startPos, recordCount) {
        console.log(`📖 워커 ${this.workerId}: ${file.name}에서 위치 ${startPos}부터 ${recordCount}개 레코드 읽기`);
        
        const reader = new jbfilereader(file, false);
        const records = [];
        
        // 해당 위치로 이동
        reader.fpos = startPos;
        
        // 지정된 개수만큼 레코드 읽기
        for (let i = 0; i < recordCount; i++) {
            const id = await this.readLine(reader);
            if (id === "") break; // EOF
            
            const seq = await this.readLine(reader);
            const plus = await this.readLine(reader);
            const qual = await this.readLine(reader);
            
            records.push({
                id: id,
                seq: seq,
                plus: plus,
                qual: qual
            });
        }
        
        console.log(`📖 워커 ${this.workerId}: ${records.length}개 레코드 읽기 완료`);
        return records;
    }
    
    // 청크 처리 메인 함수
    async processChunk(chunkData) {
        const { joinedSequences, selectionParams, sort1Params, sort2Params, chunkId } = chunkData;
        
        console.log(`🔧 워커 ${this.workerId}: 청크 ${chunkId} 3단계 필터링 시작`);

        // 2단계: 선택 필터링
        const selectedSequences = this.filterSequences(
            joinedSequences,
            selectionParams.read1,
            selectionParams.read2,
            1, // 1개 미스매치 허용
            0, // 길이 제한 없음
            true // 패턴 사이 영역만 추출
        );

        console.log(`🔍 선택 필터링 완료: ${selectedSequences.length}개 시퀀스`);

        // 3단계: 첫 번째 정렬 필터링
        const sorted1Sequences = this.filterSequences(
            selectedSequences,
            sort1Params.read1,
            sort1Params.read2,
            1, // 1개 미스매치 허용
            sort1Params.length,
            false // 전체 시퀀스 유지
        );

        console.log(`📋 첫 번째 정렬 완료: ${sorted1Sequences.length}개 시퀀스`);

        // 4단계: 두 번째 정렬 필터링
        const sorted2Sequences = this.filterSequences(
            sorted1Sequences,
            sort2Params.read1,
            sort2Params.read2,
            1, // 1개 미스매치 허용
            sort2Params.length,
            false // 전체 시퀀스 유지
        );

        console.log(`🎯 두 번째 정렬 완료: ${sorted2Sequences.length}개 시퀀스`);

        // 중복 제거 및 카운팅
        const selectedCounts = this.countUniqueSequences(selectedSequences);
        const sorted1Counts = this.countUniqueSequences(sorted1Sequences);
        const sorted2Counts = this.countUniqueSequences(sorted2Sequences);

        return {
            // 카운트 데이터 반환
            selectedCounts: selectedCounts,
            sorted1Counts: sorted1Counts,
            sorted2Counts: sorted2Counts,
            counts: {
                joined: joinedSequences.length,
                selected: selectedSequences.length,
                sorted1: sorted1Sequences.length,
                sorted2: sorted2Sequences.length
            }
        };
    }
    
    // 메시지로 받은 청크 처리
    async processChunkFromMessage(chunkInfo) {
        try {
            console.log(`🔧 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 처리 시작`);
            
            // 동기화 검증 확인
            if (!chunkInfo.syncValidated) {
                console.warn(`청크 ${chunkInfo.chunkId}: paired-read 동기화 검증 누락`);
            }
            
            // 1. 파일에서 해당 위치의 데이터 읽기
            const [file1Records, file2Records] = await Promise.all([
                this.readChunkDataFromPosition(this.files[0], chunkInfo.file1StartPos, chunkInfo.recordCount),
                this.readChunkDataFromPosition(this.files[1], chunkInfo.file2StartPos, chunkInfo.recordCount)
            ]);
            
            // 2. 추가 안전성 검증
            if (file1Records.length !== file2Records.length) {
                throw new Error(`청크 ${chunkInfo.chunkId}: 읽어온 레코드 수 불일치 (FASTQ1: ${file1Records.length}, FASTQ2: ${file2Records.length})`);
            }
            
            // 3. Paired-Read 조인 (CLAUDE.md 가이드라인: run_fastq_join 사용)
            // Paired-read 검증 수행
            let validationErrors = 0;
            for (let i = 0; i < file1Records.length; i++) {
                try {
                    this.validatePairedReads(file1Records[i], file2Records[i], i);
                } catch (error) {
                    validationErrors++;
                }
            }
            
            // run_fastq_join으로 조인 수행
            const joinedSequences = await this.runFastqJoinOnChunk(
                file1Records, 
                file2Records, 
                this.analysisParams.is_short
            );
            
            // run_fastq_join 결과를 processChunk에 맞는 형태로 변환
            const processableSequences = joinedSequences.map((seq, index) => ({
                seq: seq,
                qual: '',
                id: `joined_${index}`,
                validated: true,
                recordIndex: index
            }));
            
            // 4. 3단계 필터링
            const result = await this.processChunk({
                joinedSequences: processableSequences,
                selectionParams: {
                    read1: this.analysisParams.sel_read1,
                    read2: this.analysisParams.sel_read2
                },
                sort1Params: {
                    read1: this.analysisParams.s1_read1,
                    read2: this.analysisParams.s1_read2,
                    length: this.analysisParams.s1_l
                },
                sort2Params: {
                    read1: this.analysisParams.s2_read1,
                    read2: this.analysisParams.s2_read2,
                    length: this.analysisParams.s2_l
                },
                chunkId: chunkInfo.chunkId
            });
            
            // 검증 통계 추가
            result.validationStats = {
                totalRecords: file1Records.length,
                validRecords: joinedSequences.length,
                errorRecords: validationErrors,
                errorRate: (validationErrors / file1Records.length * 100).toFixed(2) + '%'
            };
            
            // 5. 결과를 메인 스레드로 전송
            self.postMessage({
                type: 'chunk_processed',
                workerId: this.workerId,
                chunkId: chunkInfo.chunkId,
                data: result
            });
            
            console.log(`✅ 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 처리 완료`);
            
        } catch (error) {
            console.error(`❌ 워커 ${this.workerId}: 청크 ${chunkInfo.chunkId} 처리 오류:`, error);
            throw error;
        }
    }
    
    // 시퀀스 필터링 함수
    filterSequences(sequences, pattern1, pattern2, maxMismatches = 1, requiredLength = 0, onlyBetween = false) {
        const results = [];
        const p1Len = pattern1.length;
        const p2Len = pattern2.length;
        
        for (const seq of sequences) {
            const seqStr = seq.seq || seq;
            const seqLen = seqStr.length;
            let found = false;
            
            outerLoop:
            for (let i = 0; i <= seqLen - p1Len; i++) {
                if (this.matchesWithMismatches(seqStr, pattern1, i, maxMismatches)) {
                    for (let j = i + p1Len; j <= seqLen - p2Len; j++) {
                        if (this.matchesWithMismatches(seqStr, pattern2, j, maxMismatches)) {
                            const betweenLength = j - (i + p1Len);
                            if (requiredLength === 0 || betweenLength === requiredLength) {
                                if (onlyBetween) {
                                    results.push({
                                        seq: seqStr.slice(i, j + p2Len),
                                        qual: seq.qual ? seq.qual.slice(i, j + p2Len) : '',
                                        id: seq.id || '',
                                        original: seq
                                    });
                                } else {
                                    results.push(seq);
                                }
                                found = true;
                                break outerLoop;
                            }
                        }
                    }
                }
            }
        }
        
        return results;
    }
    
    // 미스매치를 허용하여 패턴 매칭
    matchesWithMismatches(sequence, pattern, startPos, maxMismatches) {
        let mismatches = 0;
        for (let i = 0; i < pattern.length; i++) {
            if (sequence[startPos + i] !== pattern[i]) {
                mismatches++;
                if (mismatches > maxMismatches) {
                    return false;
                }
            }
        }
        return true;
    }
    
    // 중복 제거 및 카운팅
    countUniqueSequences(sequences) {
        const countMap = new Map();
        
        for (const sequence of sequences) {
            const seq = sequence.seq || sequence;
            if (countMap.has(seq)) {
                countMap.set(seq, countMap.get(seq) + 1);
            } else {
                countMap.set(seq, 1);
            }
        }
        
        // 카운트 순으로 정렬하여 배열로 변환
        return Array.from(countMap.entries())
            .map(([sequence, count]) => ({ sequence, count }))
            .sort((a, b) => b.count - a.count);
    }
}

// 워커 메인 로직
const processor = new PullBasedProcessingWorker();

self.addEventListener('message', async function(event) {
    const { type, workerId, files, analysisParams, totalChunks, totalRecords, chunkInfo } = event.data;
    
    if (type === 'start_processing') {
        // 워커 초기화
        processor.workerId = workerId;
        processor.files = files;
        processor.analysisParams = analysisParams;
        
        console.log(`🚀 워커 ${workerId}: 처리 시작 준비 완료`);
        
    } else if (type === 'process_chunk') {
        // 개별 청크 처리
        console.log(`📤 워커 ${processor.workerId}: 청크 ${chunkInfo.chunkId} 처리 시작`);
        
        try {
            await processor.processChunkFromMessage(chunkInfo);
        } catch (error) {
            console.error(`워커 ${processor.workerId}: 청크 ${chunkInfo.chunkId} 처리 오류:`, error);
            self.postMessage({
                type: 'chunk_error',
                chunkId: chunkInfo.chunkId,
                error: error.message
            });
        }
        
    } else if (type === 'chunking_complete') {
        // 청킹 완료 알림
        processor.chunkingComplete = true;
        processor.totalChunks = totalChunks;
        
        console.log(`📋 워커 ${processor.workerId}: 청킹 완료 알림 수신 (총 ${totalChunks}개 청크)`);
    }
});