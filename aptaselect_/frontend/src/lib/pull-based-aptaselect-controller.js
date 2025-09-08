// Pull 기반 AptaSelect 메인 컨트롤러 - CPU 코어 수 기반 최적화
export class PullBasedAptaSelectController {
    constructor() {
        this.chunkingWorker = null;
        this.processingWorkers = [];
        
        // 카운트 집계
        this.counts = {
            total: 0,
            selected: 0,
            sorted1: 0,
            sorted2: 0
        };
        
        // 중복 제거된 시퀀스 카운트 저장
        this.sequenceCounts = {
            selected: new Map(),
            sorted1: new Map(),
            sorted2: new Map()
        };
        
        // 진행률 추적
        this.progress = {
            chunking: 0,
            processing: 0,
            overall: 0
        };
        
        // 콜백 함수들
        this.onProgressUpdate = null;
        this.onCountUpdate = null;
        this.onComplete = null;
        this.onError = null;
        
        // 처리 상태
        this.isProcessing = false;
        this.isAnalysisComplete = false;
        this.totalChunks = 0;
        this.processedChunks = 0;
        this.completedWorkers = new Set();
        
        // 동시성 제어
        this.isAggregating = false;
        this.pendingResults = [];
        
        // 워커 관리
        this.availableWorkers = [];
        this.busyWorkers = new Set();
        
        // 공유 큐 관리 (폴백용)
        this.sharedChunkInfoQueue = [];  // 위치 정보 공유 큐
        this.queueLock = false;          // 동시성 제어용 락
        
        // 🔧 워커 완료 신호 누락 문제 해결: 청크 기반 완료 추적
        this.chunkCompletionMap = new Map(); // 청크ID -> 완료상태
        this.lastChunkProcessedTime = Date.now();
        this.lastProgressTime = Date.now();
        this.allWorkersIdleTime = null;
        this.completionCheckStartTime = null;
        
        // 💓 하트비트 관리
        this.workerHeartbeats = new Map(); // workerId -> 마지막 하트비트 시간
        this.workerStates = new Map(); // workerId -> 워커 상태 정보
    }
    
    // 청크를 가용한 워커에게 배분 (폴백 모드에서만 사용)
    distributeChunkToWorker(chunkInfo) {
        // 가용한 워커 찾기
        let availableWorker = null;
        
        for (let i = 0; i < this.processingWorkers.length; i++) {
            if (!this.busyWorkers.has(i)) {
                availableWorker = i;
                break;
            }
        }
        
        if (availableWorker !== null) {
            // 워커를 바쁜 상태로 설정
            this.busyWorkers.add(availableWorker);
            
            // 청크를 워커에게 전송
            this.processingWorkers[availableWorker].postMessage({
                type: 'process_chunk',
                chunkInfo: chunkInfo
            });
            
            console.log(`📤 폴백: 청크 ${chunkInfo.chunkId}을 워커 ${availableWorker}에게 할당`);
        } else {
            // 모든 워커가 바쁜 경우 나중에 처리하기 위해 큐에 저장
            if (!this.pendingChunks) this.pendingChunks = [];
            this.pendingChunks.push(chunkInfo);
            console.log(`⏳ 폴백: 청크 ${chunkInfo.chunkId} 대기 큐에 추가`);
        }
    }

    async startOptimizedAnalysis(files, analysisParams) {
        try {
            this.isProcessing = true;
            this.resetCounts();
            
            // 파일과 매개변수 저장 (워커에게 전달하기 위해)
            this.files = files;
            this.analysisParams = analysisParams;
            
            console.log('🚀 Pull 기반 AptaSelect 분석 시작');
            
            // 1. CPU 코어 수 기반 처리 워커 풀 준비
            const cpuCores = navigator.hardwareConcurrency || 4;
            const processingWorkerCount = Math.max(1, cpuCores - 1); // 청킹 워커 1개 제외
            
            console.log(`💻 시스템 CPU 코어: ${cpuCores}, 처리 워커: ${processingWorkerCount}개`);
            
            // 2. 처리 워커들 생성
            this.processingWorkers = [];
            for (let i = 0; i < processingWorkerCount; i++) {
                const worker = new Worker(new URL('./pull-based-processing-worker.js', import.meta.url), { type: 'module' });
                worker.id = i;
                worker.onmessage = (event) => this.handleWorkerMessage(event, i);
                worker.onerror = (error) => {
                    console.error(`워커 ${i} 오류:`, error);
                };
                this.processingWorkers.push(worker);
                console.log(`👷 처리 워커 ${i} 생성 완료`);
            }
            
            // 3. 동기화된 청킹 워커 시작
            this.chunkingWorker = new Worker(new URL('./synchronized-chunking-worker.js', import.meta.url), { type: 'module' });
            this.chunkingWorker.onmessage = (event) => this.handleChunkingWorkerMessage(event);
            this.chunkingWorker.onerror = (error) => {
                console.error('청킹 워커 오류:', error);
                if (this.onError) this.onError(error);
            };
            
            // 4. Pull 기반 스트리밍 처리 시작
            this.startPullBasedProcessing(files, analysisParams);
            
        } catch (error) {
            console.error('분석 시작 오류:', error);
            if (this.onError) {
                this.onError(error);
            }
            this.isProcessing = false;
        }
    }
    
    startPullBasedProcessing(files, analysisParams) {
        // 청킹 워커 시작
        console.log('📋 청킹 워커 시작');
        this.chunkingWorker.postMessage({
            type: 'start_chunking',
            file1: files[0],
            file2: files[1],
            chunkSize: 10000
        });
    }
    
    // 청킹 워커 메시지 처리
    handleChunkingWorkerMessage(event) {
        const { type } = event.data;
        
        if (type === 'shared_queue_ready') {
            const { sharedBuffer } = event.data;
            
            console.log('📋 SharedArrayBuffer 공유 큐 준비 완료, 처리 워커들에게 전달');
            
            // SharedArrayBuffer Pull 기반 워커 시작
            this.processingWorkers.forEach((worker, index) => {
                worker.postMessage({
                    type: 'start_processing',
                    workerId: index,
                    files: [this.files[0], this.files[1]],
                    analysisParams: this.analysisParams,
                    sharedBuffer: sharedBuffer,
                    mode: 'pull' // Pull 모드 명시
                });
                
                console.log(`👷 Pull 기반 워커 ${index} 시작됨 (SharedArrayBuffer)`);
            });
            
        } else if (type === 'fallback_queue_ready') {
            console.log('📋 폴백 큐 준비 완료, 실시간 Pull 방식 (BroadcastChannel) 시작');
            
            // 폴백: BroadcastChannel 기반 Pull 워커 시작
            this.processingWorkers.forEach((worker, index) => {
                worker.postMessage({
                    type: 'start_processing',
                    workerId: index,
                    files: [this.files[0], this.files[1]],
                    analysisParams: this.analysisParams,
                    mode: 'pull_fallback' // Pull 폴백 모드 명시
                });
                
                console.log(`👷 Pull 기반 워커 ${index} 시작됨 (BroadcastChannel 폴백)`);
            });
            
        } else if (type === 'chunk_ready_for_assignment') {
            // ✅ 청크 위치 정보 완성 즉시 공유 큐에 추가
            const { chunkInfo } = event.data;
            console.log(`⚡ 청킹 워커에서 청크 위치 정보 수신:`, chunkInfo);
            if (chunkInfo && chunkInfo.chunkId !== undefined) {
                console.log(`✅ 유효한 청크 정보 ${chunkInfo.chunkId} - 공유 큐에 추가`);
                this.addChunkInfoToSharedQueue(chunkInfo);
            } else {
                console.error(`❌ 청킹 워커에서 잘못된 청크 정보:`, chunkInfo);
            }
            
        } else if (type === 'chunk_available') {
            // 폴백 모드에서만 개별 청크 전송 처리
            const { chunkInfo } = event.data;
            this.distributeChunkToWorker(chunkInfo);
            
        } else if (type === 'chunking_progress') {
            // 청킹 진행률 업데이트
            const { progress, processedRecords, currentChunks, estimatedTotalChunks } = event.data;
            console.log(`📈 청킹 진행률 업데이트: ${progress.toFixed(1)}% (${processedRecords}개 레코드, ${currentChunks}개 청크)`);
            
            this.updateProgress('chunking', progress);
            this.processedChunks = Math.max(this.processedChunks || 0, currentChunks);
            
            // 추정 총 청크 수 업데이트 (더 정확한 값이 있으면)
            if (estimatedTotalChunks && estimatedTotalChunks > this.totalChunks) {
                this.totalChunks = estimatedTotalChunks;
            }
            
        } else if (type === 'chunking_complete') {
            const { totalChunks, totalRecords } = event.data;
            this.totalChunks = totalChunks;
            this.chunkingComplete = true;
            
            console.log(`✅ 청킹 완료: ${totalChunks}개 청크, ${totalRecords}개 레코드`);
            this.updateProgress('chunking', 100); // 청킹 완료
            
            // 모든 처리 워커에게 청킹 완료 알림
            this.processingWorkers.forEach(worker => {
                worker.postMessage({
                    type: 'chunking_complete',
                    totalChunks,
                    totalRecords
                });
            });
            
            // 완료 상태 확인
            this.checkAllWorkersComplete();
            
        } else if (type === 'chunking_error') {
            console.error('청킹 오류:', event.data.error);
            if (this.onError) {
                this.onError(new Error(event.data.error));
            }
        }
    }
    
    // 처리 워커 메시지 핸들러
    async handleWorkerMessage(event, workerId) {
        const { type, data } = event.data;
        
        if (type === 'chunk_processed') {
            // 청크 처리 완료
            this.handleChunkComplete(data, event.data.chunkId, workerId);
            
        } else if (type === 'progress_update') {
            // 진행률 업데이트
            this.handleProgressUpdate(data, workerId);
            
        } else if (type === 'count_update') {
            // 카운트 업데이트
            this.handleCountUpdate(data, workerId);
            
        } else if (type === 'worker_idle') {
            // 워커가 더 이상 처리할 청크가 없어 대기 상태
            console.log(`💤 워커 ${workerId} 대기 상태 (큐 비어있음)`);
            
        } else if (type === 'all_chunks_processed') {
            // 워커가 모든 청크 처리 완료
            console.log(`✅ 워커 ${workerId} 모든 작업 완료`);
            this.busyWorkers.delete(workerId);
            this.completedWorkers.add(workerId);
            this.checkAllWorkersComplete();
            
        } else if (type === 'chunk_error') {
            console.error(`워커 ${workerId} 청크 ${event.data.chunkId} 오류:`, event.data.error);
            
        } else if (type === 'worker_heartbeat') {
            // 💓 워커 하트비트 처리
            this.handleWorkerHeartbeat(event.data);
            
        } else if (type === 'pull_request') {
            // ⚙️ 워커가 직접 Pull 요청 (진정한 Pull 방식)
            console.log(`📨 워커 ${workerId}: Pull 요청 수신`);
            try {
                await this.handleWorkerPullRequest(workerId);
                console.log(`✅ 워커 ${workerId}: Pull 요청 처리 완료`);
            } catch (error) {
                console.error(`❌ 워커 ${workerId}: Pull 요청 처리 오류:`, error);
            }
            
        } else if (type === 'chunk_ready_for_assignment') {
            // ⚙️ 청크 위치 정보 완성 즉시 공유 큐에 추가
            const { chunkInfo } = event.data;
            console.log(`⚡ 청크 위치 정보 ${chunkInfo.chunkId} 수신 - 공유 큐에 즉시 추가`);
            this.addChunkInfoToSharedQueue(chunkInfo);
        }
    }
    
    // 청크 처리 완료 핸들러 (완료 추적 개선)
    async handleChunkComplete(result, chunkId, workerId) {
        try {
            // 🔧 청크 완료 기록 (워커 완료 신호 누락 문제 해결)
            this.chunkCompletionMap.set(chunkId, true);
            this.lastChunkProcessedTime = Date.now();
            this.lastProgressTime = Date.now();
            
            // 워커에서 받은 카운트 데이터 집계
            await this.aggregateSequenceCounts('selected', result.selectedCounts || []);
            await this.aggregateSequenceCounts('sorted1', result.sorted1Counts || []);
            await this.aggregateSequenceCounts('sorted2', result.sorted2Counts || []);
            
            // 전체 카운트 업데이트
            this.counts.selected += result.counts.selected || 0;
            this.counts.sorted1 += result.counts.sorted1 || 0;
            this.counts.sorted2 += result.counts.sorted2 || 0;
            this.counts.total += result.counts.joined || 0;
            
            this.processedChunks++;
            
            // 처리 진행률 계산 (더 자주 업데이트)
            if (this.totalChunks > 0) {
                const processingProgress = (this.processedChunks / this.totalChunks) * 100; // 0-100%
                this.updateProgress('processing', processingProgress);
            } else {
                // totalChunks를 모르는 경우 추정치 사용
                const estimatedProgress = Math.min(95, (this.processedChunks / Math.max(this.processedChunks + 10, 100)) * 100);
                this.updateProgress('processing', estimatedProgress);
            }
            
            // 카운트 업데이트 콜백 호출
            if (this.onCountUpdate) {
                this.onCountUpdate(this.counts);
            }
            
            console.log(`📊 청크 ${chunkId} (워커 ${workerId}) 처리 완료 (${this.processedChunks}/${this.totalChunks}, 완료 추적: ${this.chunkCompletionMap.size})`);
            
            // 🔧 Pull 기반: 워커가 스스로 다음 작업 찾으므로 별도 할당 불필요
            // 워커는 처리 완료 후 자동으로 큐에서 다음 청크 가져감
            console.log(`🔄 Pull 기반: 워커 ${workerId} 자동으로 다음 작업 처리 예정`);
            
            // 더 이상 busyWorkers나 pendingChunks 관리 불필요
            
            // 🔧 완료 상태 확인 (청크 기반 완료 추적 사용)
            if (this.chunkingComplete && this.allChunksCompleted()) {
                console.log('🎉 완료 감지: 모든 청크 처리 완료 확인됨');
                this.finalizeResults();
                return;
            }
            
            // 모든 작업이 완료되었는지 확인
            this.checkAllWorkersComplete();
            
        } catch (error) {
            console.error(`청크 ${chunkId} 완료 처리 오류:`, error);
        }
    }
    
    // 🔧 모든 청크 완료 여부 확인
    allChunksCompleted() {
        const completedChunks = this.chunkCompletionMap.size;
        const result = this.totalChunks && completedChunks >= this.totalChunks;
        
        if (this.totalChunks) {
            console.log(`🔍 청크 완료 상태: ${completedChunks}/${this.totalChunks} (${result ? '완료' : '진행중'})`);
        }
        
        return result;
    }
    
    // ⚙️ 공유 큐에 청크 위치 정보 추가 (폴백 모드 실시간 스트리밍)
    async addChunkInfoToSharedQueue(chunkInfo) {
        // 동시성 제어
        while (this.queueLock) {
            await new Promise(resolve => setTimeout(resolve, 1));
        }
        this.queueLock = true;
        
        try {
            this.sharedChunkInfoQueue.push(chunkInfo);
            console.log(`📍 청크 위치 정보 ${chunkInfo.chunkId} 공유 큐에 추가`);
            console.log(`   📈 공유 큐 크기: ${this.sharedChunkInfoQueue.length}개 위치 정보`);
            console.log(`   📍 내용: FASTQ1 ${chunkInfo.file1StartPos}, FASTQ2 ${chunkInfo.file2StartPos}, ${chunkInfo.recordCount}개 레코드`);
        } finally {
            this.queueLock = false;
        }
    }
    
    // ⚙️ 워커 Pull 요청 처리 (진정한 Pull 방식)
    async handleWorkerPullRequest(workerId) {
        console.log(`📨 워커 ${workerId} Pull 요청 처리 시작 (큐 크기: ${this.sharedChunkInfoQueue.length})`);
        
        const chunkInfo = await this.dequeueChunkInfo();
        const worker = this.processingWorkers[workerId];
        
        if (chunkInfo) {
            // Pull 요청한 워커에게 위치 정보 전송
            console.log(`✅ 워커 ${workerId}에게 청크 정보 전송:`, chunkInfo);
            worker.postMessage({
                type: 'chunk_info_response',
                workerId: workerId,
                chunkInfo: chunkInfo
            });
            
            console.log(`🔽 워커 ${workerId}: Pull 요청 → 청크 위치 정보 ${chunkInfo.chunkId} 제공`);
            
        } else {
            // 큐가 비어있음을 알림
            worker.postMessage({
                type: 'queue_empty',
                workerId: workerId
            });
            
            console.log(`💤 워커 ${workerId}: Pull 요청 → 큐 비어있음 알림`);
        }
    }
    
    // ⚙️ 공유 큐에서 청크 위치 정보 추출 (원자적 연산)
    async dequeueChunkInfo() {
        while (this.queueLock) {
            await new Promise(resolve => setTimeout(resolve, 1));
        }
        this.queueLock = true;
        
        let chunkInfo = null;
        try {
            if (this.sharedChunkInfoQueue.length > 0) {
                chunkInfo = this.sharedChunkInfoQueue.shift();
                console.log(`🔽 공유 큐에서 청크 위치 정보 ${chunkInfo.chunkId} 추출 (남은: ${this.sharedChunkInfoQueue.length})`);
            } else {
                console.log(`💤 공유 큐가 비어있음 (큐 크기: ${this.sharedChunkInfoQueue.length})`);
            }
        } finally {
            this.queueLock = false;
        }
        
        return chunkInfo;
    }
    
    // 💓 워커 하트비트 처리
    handleWorkerHeartbeat(heartbeatData) {
        const { workerId, isIdle, processedChunks, queueEmpty, chunkingComplete, timestamp } = heartbeatData;
        
        // 하트비트 시간 기록
        this.workerHeartbeats.set(workerId, timestamp);
        
        // 워커 상태 업데이트
        this.workerStates.set(workerId, {
            isIdle,
            processedChunks,
            queueEmpty,
            chunkingComplete,
            lastHeartbeat: timestamp
        });
        
        // 주기적으로 하트비트 로그 출력 (30초마다)
        if (timestamp % 30000 < 5000) {
            console.log(`💓 워커 ${workerId} 하트비트: ${isIdle ? '유휴' : '바쁨'}, 처리 청크: ${processedChunks}, 큐 비어있음: ${queueEmpty}`);
        }
    }
    
    // 모든 워커가 유휴 상태인지 하트비트로 확인
    allWorkersReportIdle() {
        if (this.workerStates.size === 0) return false;
        
        for (const [workerId, state] of this.workerStates) {
            if (!state.isIdle || !state.queueEmpty) {
                return false;
            }
        }
        return true;
    }
    
    // 하트비트 기반 완료 감지
    noProgressFor5Seconds() {
        const now = Date.now();
        
        // 모든 워커의 마지막 하트비트가 5초 이상 동일한 상태인지 확인
        for (const [workerId, state] of this.workerStates) {
            const timeSinceLastHeartbeat = now - (state.lastHeartbeat || 0);
            if (timeSinceLastHeartbeat > 5000 && !state.isIdle) {
                // 5초 이상 응답이 없고 바쁜 상태인 워커가 있음
                return false;
            }
        }
        
        return this.lastProgressTime && (now - this.lastProgressTime) > 5000;
    }
    
    // 시퀀스 카운트 집계
    async aggregateSequenceCounts(type, newCounts) {
        // 동시성 제어
        if (this.isAggregating) {
            this.pendingResults.push({ type, newCounts });
            return;
        }
        
        this.isAggregating = true;
        
        try {
            const targetMap = this.sequenceCounts[type];
            
            console.log(`${type} 집계 중: ${newCounts.length}개 고유 시퀀스 받음`);
            
            for (const { sequence, count } of newCounts) {
                if (targetMap.has(sequence)) {
                    targetMap.set(sequence, targetMap.get(sequence) + count);
                } else {
                    targetMap.set(sequence, count);
                }
            }
            
            console.log(`${type} 집계 완료: 현재 ${targetMap.size}개 고유 시퀀스`);
            
            // 대기 중인 결과들 처리
            while (this.pendingResults.length > 0) {
                const pending = this.pendingResults.shift();
                await this.aggregateSequenceCounts(pending.type, pending.newCounts);
            }
            
        } finally {
            this.isAggregating = false;
        }
    }
    
    // 🔧 모든 워커 완료 확인 (다중 완료 감지 메커니즘)
    checkAllWorkersComplete() {
        const now = Date.now();
        // 🔧 Pull 기반에서는 busyWorkers, pendingChunks 사용하지 않음
        const chunkingFinished = this.chunkingComplete;
        const allChunksProcessed = this.totalChunks && this.processedChunks >= this.totalChunks;
        const allWorkersCompleted = this.completedWorkers.size >= this.processingWorkers.length;
        
        // 🔧 새로운 대안 완료 조건들
        const allChunksCompletedByTracking = this.chunkingComplete && this.allChunksCompleted();
        const noProgressFor10Seconds = this.lastProgressTime && (now - this.lastProgressTime) > 10000;
        const noChunkProcessedFor5Seconds = this.lastChunkProcessedTime && (now - this.lastChunkProcessedTime) > 5000;
        
        // 🔧 Pull 기반에서는 워커 유휴 상태를 하트비트로 추적
        // 모든 워커가 하트비트에서 isIdle=true, queueEmpty=true 보고하는지 확인
        const allWorkersReportIdle = this.allWorkersReportIdle();
        
        const allWorkersIdleFor5Seconds = allWorkersReportIdle && this.noProgressFor5Seconds();
        
        // 🔧 하트비트 기반 완료 감지
        const heartbeatBasedCompletion = this.allWorkersReportIdle() && chunkingFinished && this.noProgressFor5Seconds();
        
        console.log(`🔍 Pull 기반 워커 완료 상태 상세 확인:`);
        console.log(`   • 청킹 완료: ${chunkingFinished}`);
        console.log(`   • 하트비트 워커 유휴: ${allWorkersReportIdle} (${this.workerStates.size}/${this.processingWorkers.length})`);
        console.log(`   • 처리된 청크: ${this.processedChunks}/${this.totalChunks || 'unknown'}`);
        console.log(`   • 완료 추적된 청크: ${this.chunkCompletionMap.size}/${this.totalChunks || 'unknown'}`);
        console.log(`   • 완료된 워커: ${this.completedWorkers.size}/${this.processingWorkers.length}`);
        console.log(`   • 마지막 진전: ${Math.round((now - this.lastProgressTime) / 1000)}초 전`);
        console.log(`   • 마지막 청크 처리: ${Math.round((now - this.lastChunkProcessedTime) / 1000)}초 전`);
        console.log(`   • 하트비트 기반 완료: ${heartbeatBasedCompletion}`);
        
        // 🔧 완료 조건 우선순위 (가장 신뢰할 수 있는 조건부터)
        if (allChunksCompletedByTracking) {
            console.log('🎉 완료 조건A: 청크 기반 완료 추적 확인됨');
            this.finalizeResults();
        } else if (allWorkersCompleted && chunkingFinished) {
            console.log('🎉 완료 조건B: 워커 완료 신호 수신');
            this.finalizeResults();
        } else if (heartbeatBasedCompletion) {
            console.log('🎉 완료 조건C: 하트비트 기반 완료 감지');
            this.finalizeResults();
        } else if (allChunksProcessed && chunkingFinished && allWorkersReportIdle) {
            console.log('🎉 완료 조건D: 모든 청크 처리 + 워커 유휴 (하트비트)');
            this.finalizeResults();
        } else if (chunkingFinished && allWorkersIdleFor5Seconds) {
            console.log('🎉 완료 조건E: 5초간 워커 유휴 상태 (하트비트)');
            this.finalizeResults();
        } else if (chunkingFinished && allWorkersReportIdle && noChunkProcessedFor5Seconds) {
            console.log('🎉 완료 조건F: 5초간 청크 처리 없음');
            this.finalizeResults();
        } else {
            // 🔧 무한 대기 방지: 더 짧은 타임아웃 (15초)
            if (chunkingFinished && !this.completionCheckStartTime) {
                this.completionCheckStartTime = now;
                console.log('⏰ 완료 검사 타이머 시작 (15초 후 강제 완료)');
            }
            
            if (this.completionCheckStartTime && (now - this.completionCheckStartTime) > 15000) {
                console.warn('⚠️ 15초간 완료 대기 - 강제 완료 실행');
                console.log(`   현재 상태: 처리된 청크 ${this.processedChunks}, 추적된 청크 ${this.chunkCompletionMap.size}, 활성 워커 ${this.busyWorkers.size}`);
                this.finalizeResults();
            } else {
                const remainingTime = this.completionCheckStartTime ? 
                    Math.round((15000 - (now - this.completionCheckStartTime)) / 1000) : 
                    '타이머 시작 전';
                console.log(`⏳ 처리 계속 중... (강제 완료까지 ${remainingTime}초)`);
            }
        }
    }
    
    // 최종 결과 집계
    async finalizeResults() {
        try {
            // 분석 완료 플래그 설정
            this.isAnalysisComplete = true;
            
            // 메모리에서 상위 시퀀스 생성
            const topSequences = {
                selected: this.getTopSequencesFromMap('selected', 10),
                sorted1: this.getTopSequencesFromMap('sorted1', 10),
                sorted2: this.getTopSequencesFromMap('sorted2', 10)
            };
            
            this.counts.topSequences = topSequences;
            
            this.updateProgress('processing', 100);
            
            console.log('🎊 최종 집계 완료:', this.counts);
            console.log('상위 시퀀스 개수:', {
                selected: topSequences.selected.length,
                sorted1: topSequences.sorted1.length,
                sorted2: topSequences.sorted2.length
            });
            
            if (this.onComplete) {
                this.onComplete(this.counts);
            }
            
        } catch (error) {
            console.error('최종 집계 오류:', error);
            if (this.onError) {
                this.onError(error);
            }
        } finally {
            this.isProcessing = false;
            this.terminateWorkers();
        }
    }
    
    // Map에서 상위 N개 시퀀스 추출
    getTopSequencesFromMap(type, limit) {
        const countMap = this.sequenceCounts[type];
        const sorted = Array.from(countMap.entries())
            .map(([sequence, count]) => ({ sequence, count }))
            .sort((a, b) => b.count - a.count);
            
        // limit이 -1이면 전체 반환, 그렇지 않으면 상위 N개만 반환
        return limit === -1 ? sorted : sorted.slice(0, limit);
    }
    
    // 진행률 업데이트 (전체 파일 기준으로 수정)
    updateProgress(stage, value) {
        this.progress[stage] = value;
        this.lastProgressTime = Date.now();
        
        // 전체 진행률 계산 (청킹과 처리 동시 고려)
        this.progress.overall = this.calculateOverallProgress();
        
        // 각 단계별 진행률을 전체 파일 기준으로 계산 (수정)
        const progressInfo = this.createProgressInfo();
        
        if (this.onProgressUpdate) {
            this.onProgressUpdate(progressInfo);
        }
        
        console.log(`📊 진행률 업데이트 [${stage}]: ${value.toFixed(1)}% → 전체: ${this.progress.overall.toFixed(1)}%`);
    }
    
    // 전체 진행률을 더 정교하게 계산
    calculateOverallProgress() {
        const chunkingPercent = this.progress.chunking || 0;
        const processingPercent = this.progress.processing || 0;
        
        // Pull 기반에서는 청킹과 처리가 동시 진행
        // 청킹이 완료되지 않았으면 청킹 진행률을 기준으로 계산
        if (chunkingPercent < 100) {
            // 청킹 중: 청킹 진행률 × 0.6 + 처리 진행률 × 0.4
            return Math.min(100, chunkingPercent * 0.6 + processingPercent * 0.4);
        } else {
            // 청킹 완료: 60% + 처리 진행률 × 0.4
            return Math.min(100, 60 + processingPercent * 0.4);
        }
    }
    
    // 실시간 처리 속도 계산 및 progressInfo 생성 (updateProgress에서 분리)
    createProgressInfo() {
        // 실시간 처리 속도 계산
        const now = Date.now();
        if (this.lastProgressUpdate && this.processedChunks > 0) {
            const timeDiff = (now - this.lastProgressUpdate) / 1000; // 초
            if (timeDiff > 0) {
                const recentChunks = this.processedChunks - (this.lastProcessedChunks || 0);
                this.processingSpeed = Math.round(recentChunks / timeDiff);
                this.lastProcessedChunks = this.processedChunks;
            }
        }
        this.lastProgressUpdate = now;
        
        // 남은 시간 추정 (처리 속도가 있을 때만)
        let estimatedTimeRemaining = null;
        if (this.processingSpeed > 0 && this.totalChunks && this.totalChunks !== 'unknown') {
            const remainingChunks = this.totalChunks - this.processedChunks;
            if (remainingChunks > 0) {
                estimatedTimeRemaining = Math.ceil(remainingChunks / this.processingSpeed);
            }
        }
        
        // 전체 파일에서 각 단계가 차지하는 비중:
        // 청킹: 60%, 조인: 16%, 선택: 12%, 정렬1: 8%, 정렬2: 3%, 집계: 1%
        
        const chunkingPercent = this.progress.chunking || 0;
        const processingPercent = this.progress.processing || 0;
        
        // 각 처리 단계별 가중치 (processing 40% 내에서 분배)
        const processingWeights = {
            joining: 0.4,    // 조인: 40% (16%/40%)
            selecting: 0.3,  // 선택: 30% (12%/40%) 
            sorting1: 0.2,   // 정렬1: 20% (8%/40%)
            sorting2: 0.075, // 정렬2: 7.5% (3%/40%)
            aggregating: 0.025 // 집계: 2.5% (1%/40%)
        };
        
        // 전체 파일 기준 각 단계 진행률 계산
        const fileBasedProgress = {
            chunking: chunkingPercent * 0.6,  // 전체의 60%
            joining: processingPercent * processingWeights.joining * 0.4,
            selecting: processingPercent * processingWeights.selecting * 0.4,
            sorting1: processingPercent * processingWeights.sorting1 * 0.4,
            sorting2: processingPercent * processingWeights.sorting2 * 0.4,
            aggregating: processingPercent * processingWeights.aggregating * 0.4
        };
        
        // 추가 정보 계산 (CLAUDE.md 개선사항 포함)
        return {
            // 원본 진행률 (기존 호환성)
            ...this.progress,
            
            // 전체 파일 기준 단계별 진행률 (새로 추가)
            stageProgress: fileBasedProgress,
            
            // 각 단계별 진행률을 명시적으로 전달
            chunking: this.progress.chunking || 0,
            processing: this.progress.processing || 0,
            busyWorkers: this.getActiveWorkersCount(), // Pull 기반에서는 하트비트로 계산
            totalWorkers: this.processingWorkers.length,
            processedChunks: this.processedChunks,
            totalChunks: this.totalChunks || 'unknown',
            completedWorkers: this.completedWorkers.size,
            processingSpeed: this.processingSpeed || 0,
            estimatedTimeRemaining: estimatedTimeRemaining,
            
            // 상태 정보
            isChunking: chunkingPercent < 100,
            isProcessing: this.busyWorkers.size > 0,
            
            // 디버그 정보
            debug: {
                rawChunking: chunkingPercent,
                rawProcessing: processingPercent,
                weights: processingWeights
            }
        };
    }
    
    // 카운트 초기화 (완료 추적 변수 포함)
    resetCounts() {
        this.counts = {
            total: 0,
            selected: 0,
            sorted1: 0,
            sorted2: 0
        };
        
        // 시퀀스 카운트 Map 초기화
        this.sequenceCounts = {
            selected: new Map(),
            sorted1: new Map(),
            sorted2: new Map()
        };
        
        this.progress = {
            chunking: 0,
            processing: 0,
            overall: 0
        };
        
        this.processedChunks = 0;
        this.completedWorkers.clear();
        this.pendingResults = [];
        
        // 🔧 워커 완료 추적 변수들 초기화
        this.chunkCompletionMap.clear();
        this.lastChunkProcessedTime = Date.now();
        this.lastProgressTime = Date.now();
        this.allWorkersIdleTime = null;
        this.completionCheckStartTime = null;
        this.isAnalysisComplete = false;
        this.totalChunks = 0;
        this.chunkingComplete = false;
        
        // 파일과 매개변수는 여기서 초기화하지 않음 (startOptimizedAnalysis에서 설정됨)
    }
    
    // 분석 중단
    async stopAnalysis() {
        this.isProcessing = false;
        this.terminateWorkers();
        console.log('분석 중단됨');
    }
    
    // 워커들 종료
    terminateWorkers() {
        if (this.chunkingWorker) {
            this.chunkingWorker.terminate();
            this.chunkingWorker = null;
        }
        
        this.processingWorkers.forEach(worker => {
            worker.terminate();
        });
        this.processingWorkers = [];
    }
    
    // 전체 시퀀스 데이터 조회 (다운로드용)
    async getAllSequencesForDownload() {
        try {
            // 메모리에서 전체 시퀀스와 카운트 데이터 조회
            const allSequences = this.getTopSequencesFromMap('sorted2', -1); // -1은 전체를 의미
            
            console.log(`다운로드용 전체 시퀀스 조회 완료: ${allSequences.length}개`);
            console.log('첫 5개 시퀀스 샘플:', allSequences.slice(0, 5));
            
            return allSequences;

        } catch (error) {
            console.error('전체 시퀀스 조회 오류:', error);
            throw error;
        }
    }
    
    // 🔧 하트비트 기반 완료 감지 헬퍼 메서드들
    allWorkersReportIdle() {
        if (this.workerStates.size === 0) return false;
        
        let idleCount = 0;
        for (const [workerId, state] of this.workerStates) {
            if (state.isIdle && state.queueEmpty) {
                idleCount++;
            }
        }
        
        const allIdle = idleCount === this.processingWorkers.length;
        console.log(`🔍 하트비트 상태: ${idleCount}/${this.processingWorkers.length} 워커가 유휴 상태 보고`);
        return allIdle;
    }
    
    noProgressFor5Seconds() {
        const now = Date.now();
        const noProgress = this.lastChunkProcessedTime && (now - this.lastChunkProcessedTime) > 5000;
        if (noProgress) {
            console.log(`🔍 진행 없음: ${Math.round((now - this.lastChunkProcessedTime) / 1000)}초간 청크 처리 없음`);
        }
        return noProgress;
    }
    
    getActiveWorkersCount() {
        // Pull 기반에서는 하트비트 정보로 활성 워커 수 계산
        let activeWorkers = 0;
        for (const [workerId, state] of this.workerStates) {
            if (!state.isIdle || !state.queueEmpty) {
                activeWorkers++;
            }
        }
        return activeWorkers;
    }
    
    // 공유 큐에 위치 정보 추가
    async addChunkInfoToSharedQueue(chunkInfo) {
        // 동시성 제어 (여러 청킹 워커가 동시에 추가할 수 있음)
        while (this.queueLock) {
            await new Promise(resolve => setTimeout(resolve, 1));
        }
        this.queueLock = true;
        
        this.sharedChunkInfoQueue.push(chunkInfo);
        console.log(`📋 청크 위치 정보 ${chunkInfo.chunkId} 공유 큐에 추가`);
        console.log(`   📊 공유 큐 크기: ${this.sharedChunkInfoQueue.length}개 위치 정보`);
        
        this.queueLock = false;
    }
    
    // 워커가 Pull 요청할 때 호출되는 메서드
    async handleWorkerPullRequest(workerId) {
        const chunkInfo = await this.dequeueChunkInfo();
        const worker = this.processingWorkers[workerId];
        
        if (chunkInfo) {
            // ✅ Pull 요청한 워커에게 위치 정보 전송
            worker.postMessage({
                type: 'chunk_info_response',
                chunkInfo: chunkInfo
            });
            console.log(`🔽 워커 ${workerId}: Pull 요청 → 청크 위치 정보 ${chunkInfo.chunkId} 제공`);
        } else {
            // 큐가 비어있음을 알림
            worker.postMessage({
                type: 'queue_empty'
            });
        }
    }
    
    // 공유 큐에서 위치 정보 추출
    async dequeueChunkInfo() {
        while (this.queueLock) {
            await new Promise(resolve => setTimeout(resolve, 1));
        }
        this.queueLock = true;
        
        const chunkInfo = this.sharedChunkInfoQueue.shift();
        this.queueLock = false;
        
        return chunkInfo;
    }
    
    // 상태 조회
    getStatus() {
        return {
            isProcessing: this.isProcessing,
            progress: this.progress,
            counts: this.counts,
            totalChunks: this.totalChunks,
            processedChunks: this.processedChunks,
            activeWorkers: this.processingWorkers.length,
            completedWorkers: this.completedWorkers.size,
            cpuCores: navigator.hardwareConcurrency || 'unknown',
            queueSize: this.sharedChunkInfoQueue.length // 큐 크기 추가
        };
    }
}