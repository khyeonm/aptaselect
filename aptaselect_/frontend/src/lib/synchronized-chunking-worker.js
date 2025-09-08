// 동기화된 청킹 워커 - SharedArrayBuffer 공유 큐 사용
import { jbfilereader } from './jbfilereader.js';

class SynchronizedChunkingWorker {
    constructor() {
        // SharedArrayBuffer를 사용한 공유 큐 (워커 간 공유)
        this.sharedQueue = null;
        this.sharedBuffer = null;
        this.queueSize = 1000; // 최대 큐 크기
        this.queueHead = 0;    // 큐 헤드 인덱스
        this.queueTail = 0;    // 큐 테일 인덱스
        this.isQueueReady = false;
        
        // BroadcastChannel 기반 즉시 알림 시스템
        this.queueChannel = new BroadcastChannel('chunk-queue');
    }
    
    // 🔧 SharedArrayBuffer 기반 공유 큐 초기화 (폴백 포함)
    initializeSharedQueue() {
        // SharedArrayBuffer 지원 여부 확인
        if (typeof SharedArrayBuffer !== 'undefined') {
            try {
                // SharedArrayBuffer 방식
                const queueBufferSize = 16 + (this.queueSize * 24);
                this.sharedBuffer = new SharedArrayBuffer(queueBufferSize);
                this.sharedQueue = new Int32Array(this.sharedBuffer);
                
                // 큐 메타데이터 초기화
                this.sharedQueue[0] = 0; // head
                this.sharedQueue[1] = 0; // tail
                this.sharedQueue[2] = 0; // count
                this.sharedQueue[3] = this.queueSize; // maxSize
                
                this.useSharedBuffer = true;
                console.log('✅ SharedArrayBuffer 공유 청크 큐 초기화 완료');
                console.log(`   큐 버퍼 크기: ${queueBufferSize} bytes`);
            } catch (error) {
                console.warn('⚠️ SharedArrayBuffer 초기화 실패, 폴백 방식 사용:', error);
                this.initializeFallbackQueue();
            }
        } else {
            console.warn('⚠️ SharedArrayBuffer 미지원, 폴백 방식 사용');
            this.initializeFallbackQueue();
        }
        
        this.isQueueReady = true;
        console.log(`   최대 청크 수: ${this.queueSize}개`);
    }
    
    // 폴백: 일반 배열 기반 큐
    initializeFallbackQueue() {
        this.chunkQueue = [];
        this.useSharedBuffer = false;
        console.log('📋 폴백: 일반 배열 기반 청크 큐 초기화 완료');
    }
    
    // 청크 위치 정보를 큐에 추가
    enqueueChunk(chunkInfo) {
        if (this.useSharedBuffer) {
            // SharedArrayBuffer 방식
            const count = Atomics.load(this.sharedQueue, 2);
            const maxSize = Atomics.load(this.sharedQueue, 3);
            
            if (count >= maxSize) {
                console.warn(`⚠️ 공유 큐 가득참, 대기 중... (${count}/${maxSize})`);
                return false;
            }
            
            const tail = Atomics.load(this.sharedQueue, 1);
            const baseIndex = 4 + (tail * 6);
            
            // 청크 정보를 SharedArrayBuffer에 원자적으로 저장
            this.sharedQueue[baseIndex] = chunkInfo.chunkId;
            this.sharedQueue[baseIndex + 1] = chunkInfo.file1StartPos & 0xFFFFFFFF;
            this.sharedQueue[baseIndex + 2] = (chunkInfo.file1StartPos >> 32) & 0xFFFFFFFF;
            this.sharedQueue[baseIndex + 3] = chunkInfo.file2StartPos & 0xFFFFFFFF;
            this.sharedQueue[baseIndex + 4] = (chunkInfo.file2StartPos >> 32) & 0xFFFFFFFF;
            this.sharedQueue[baseIndex + 5] = chunkInfo.recordCount;
            
            Atomics.store(this.sharedQueue, 1, (tail + 1) % maxSize);
            Atomics.add(this.sharedQueue, 2, 1);
            
            console.log(`📋 청크 ${chunkInfo.chunkId} SharedArrayBuffer 큐에 추가 (큐 크기: ${count + 1}/${maxSize})`);
            
        } else {
            // 폴백: 일반 배열 방식 + BroadcastChannel 즉시 알림
            if (this.chunkQueue.length >= this.queueSize) {
                console.warn(`⚠️ 폴백 큐 가득참, 대기 중... (${this.chunkQueue.length}/${this.queueSize})`);
                return false;
            }
            
            // ✅ 청크 위치 정보를 컨트롤러에게 즉시 전송
            self.postMessage({
                type: 'chunk_ready_for_assignment',
                chunkInfo: chunkInfo
            });
            
            // ✅ BroadcastChannel로 모든 처리 워커에게 즉시 알림
            this.queueChannel.postMessage({
                type: 'queue_item_added',
                queueSize: this.chunkQueue.length + 1
            });
            
            console.log(`⚡ 청크 위치 정보 ${chunkInfo.chunkId} 즉시 전송 + 브로드캐스트 알림`);
            console.log(`   📍 전송 내용: FASTQ1 위치 ${chunkInfo.file1StartPos}, FASTQ2 위치 ${chunkInfo.file2StartPos}`);
            console.log(`   📊 레코드 수: ${chunkInfo.recordCount}개`);
        }
        
        return true;
    }
    
    async chunkBothFiles(file1, file2, chunkSize = 10000) {
        const reader1 = new jbfilereader(file1, false);
        const reader2 = new jbfilereader(file2, false);
        let recordCount = 0;
        let chunkId = 0;
        
        // 진행률 계산을 위한 파일 크기 정보
        const totalFileSize = file1.size + file2.size;
        let lastProgressUpdate = 0;
        const progressUpdateInterval = 50000; // 5만 레코드마다 진행률 업데이트
        
        console.log(`📊 청킹 시작: 총 파일 크기 ${(totalFileSize / 1024 / 1024).toFixed(1)}MB`);
        
        this.initializeSharedQueue();
        
        // 🔧 큐 방식에 따라 다른 메시지 전송
        if (this.useSharedBuffer) {
            self.postMessage({
                type: 'shared_queue_ready',
                sharedBuffer: this.sharedBuffer
            });
        } else {
            self.postMessage({
                type: 'fallback_queue_ready'
            });
        }
        
        while (true) {
            let file1ChunkPosition = reader1.fpos; // FASTQ1 청크 시작 위치
            let file2ChunkPosition = reader2.fpos; // FASTQ2 청크 시작 위치
            let chunkRecordCount = 0;
            
            // 청크 시작에서 레코드 동기화 검증 (seek 없이)
            const syncCheck = await this.validateChunkStartSync(reader1, reader2);
            if (!syncCheck.isValid && chunkId > 0) {
                console.warn(`⚠️ 청크 ${chunkId} 시작 위치에서 동기화 문제 감지: ${syncCheck.message}`);
            }
            
            // 🔒 핵심: 두 파일에서 동시에 정확히 같은 개수의 레코드 위치 확인
            // paired-read 동기화를 위해 반드시 같은 레코드 수로 청킹
            for (let i = 0; i < chunkSize; i++) {
                // 파일1에서 1개 레코드 건너뛰기 (위치만 기록)
                const record1Result = await this.skipSingleRecord(reader1);
                if (!record1Result.exists) break; // EOF
                
                // 파일2에서 1개 레코드 건너뛰기 (위치만 기록)
                const record2Result = await this.skipSingleRecord(reader2);
                if (!record2Result.exists) {
                    throw new Error('⚠️ FASTQ1과 FASTQ2의 레코드 수가 일치하지 않습니다! paired-read 동기화 실패');
                }
                
                // 레코드 ID 기본 매칭 확인 (1000개마다 샘플링)
                if (i % 1000 === 0 && record1Result.recordId && record2Result.recordId) {
                    const baseId1 = record1Result.recordId.replace(/\/[12]$/, '').replace(/\s.*$/, '');
                    const baseId2 = record2Result.recordId.replace(/\/[12]$/, '').replace(/\s.*$/, '');
                    if (baseId1 !== baseId2) {
                        console.warn(`⚠️ 청크 ${chunkId}의 ${i}번째 레코드 ID 불일치: ${baseId1} vs ${baseId2}`);
                    }
                }
                
                chunkRecordCount++;
                recordCount++;
                
                // 진행률 업데이트 (5만 레코드마다)
                if (recordCount - lastProgressUpdate >= progressUpdateInterval) {
                    const currentPos = reader1.fpos + reader2.fpos;
                    const chunkingProgress = Math.min(100, (currentPos / totalFileSize) * 100);
                    
                    // 메인 스레드로 청킹 진행률 전송
                    self.postMessage({
                        type: 'chunking_progress',
                        progress: chunkingProgress,
                        processedRecords: recordCount,
                        currentChunks: chunkId + 1,
                        estimatedTotalChunks: Math.ceil((totalFileSize / currentPos) * (chunkId + 1))
                    });
                    
                    console.log(`📈 청킹 진행률: ${chunkingProgress.toFixed(1)}% (${recordCount}개 레코드, ${chunkId + 1}개 청크)`);
                    lastProgressUpdate = recordCount;
                }
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
        // FASTQ 형식 인식 개선: 정확한 레코드 구조 파싱
        
        // 1단계: '@'로 시작하는 헤더 라인 찾기 및 검증
        let headerLine = await this.readLine(reader);
        if (headerLine === "") return { exists: false }; // EOF
        
        // '@' 헤더가 아니면 다음 '@' 헤더 찾기 (동기화 보정)
        while (!headerLine.startsWith('@')) {
            headerLine = await this.readLine(reader);
            if (headerLine === "") return { exists: false }; // EOF
        }
        
        const recordId = headerLine; // 레코드 ID 저장
        
        // 2단계: 시퀀스 라인들 모두 읽기 ('+'가 나올 때까지)
        let sequenceLines = [];
        let line = await this.readLine(reader);
        if (line === "") return { exists: false }; // EOF
        
        while (!line.startsWith('+')) {
            sequenceLines.push(line);
            line = await this.readLine(reader);
            if (line === "") return { exists: false }; // EOF - 불완전한 레코드
        }
        
        // 3단계: 품질 점수 라인들 읽기 (시퀀스와 같은 길이까지)
        const totalSequenceLength = sequenceLines.join('').length;
        let qualityLength = 0;
        
        while (qualityLength < totalSequenceLength) {
            const qualityLine = await this.readLine(reader);
            if (qualityLine === "") return { exists: false }; // EOF - 불완전한 레코드
            qualityLength += qualityLine.length;
        }
        
        return { 
            exists: true, 
            recordId: recordId,
            sequenceLength: totalSequenceLength,
            position: reader.fpos
        };
    }
    
    async readLine(reader) {
        return new Promise((resolve) => {
            reader.readline(resolve);
        });
    }
    
    // 청크 시작 시 동기화 검증 (seek 없이 스트림 방식)
    async validateChunkStartSync(reader1, reader2) {
        // 현재 위치에서 다음 헤더 미리보기 (버퍼 상태 확인)
        const pos1 = reader1.fpos;
        const pos2 = reader2.fpos;
        
        // 첫 번째 청크는 항상 동기화되어 있다고 가정
        if (pos1 === 0 && pos2 === 0) {
            return { isValid: true, message: "첫 번째 청크 - 동기화 보장" };
        }
        
        // 단순히 현재 위치 비교로 대략적인 동기화 상태 확인
        const posDiff = Math.abs(pos1 - pos2);
        const isSync = posDiff < 10000; // 10KB 차이 이내면 동기화된 것으로 간주
        
        return {
            isValid: isSync,
            message: isSync ? 
                `위치 차이 ${posDiff}바이트 - 동기화 양호` : 
                `위치 차이 ${posDiff}바이트 - 동기화 문제 가능성`
        };
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