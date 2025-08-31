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
    
    // 청크를 가용한 워커에게 배분
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
            
            console.log(`📤 청크 ${chunkInfo.chunkId}을 워커 ${availableWorker}에게 할당`);
        } else {
            // 모든 워커가 바쁜 경우 나중에 처리하기 위해 큐에 저장
            if (!this.pendingChunks) this.pendingChunks = [];
            this.pendingChunks.push(chunkInfo);
            console.log(`⏳ 청크 ${chunkInfo.chunkId} 대기 큐에 추가`);
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
        
        if (type === 'queue_ready') {
            console.log('📋 큐 준비 완료, 처리 워커들에게 알림');
            
            // 모든 처리 워커에게 분석 매개변수 전달
            this.processingWorkers.forEach((worker, index) => {
                worker.postMessage({
                    type: 'start_processing',
                    workerId: index,
                    files: [this.files[0], this.files[1]], // File 객체 전달
                    analysisParams: this.analysisParams
                });
                
                console.log(`👷 워커 ${index} 시작됨`);
            });
            
        } else if (type === 'chunk_available') {
            // 청크가 준비되었을 때 처리 워커에게 전달
            const { chunkInfo } = event.data;
            this.distributeChunkToWorker(chunkInfo);
            
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
    handleWorkerMessage(event, workerId) {
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
            
            // 워커를 다시 사용 가능하게 설정
            this.busyWorkers.delete(workerId);
            
            // 대기 중인 청크가 있으면 할당
            if (this.pendingChunks && this.pendingChunks.length > 0) {
                const nextChunk = this.pendingChunks.shift();
                this.distributeChunkToWorker(nextChunk);
            }
            
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
        const allWorkersIdle = this.busyWorkers.size === 0;
        const chunkingFinished = this.chunkingComplete;
        const noMoreChunks = !this.pendingChunks || this.pendingChunks.length === 0;
        const allChunksProcessed = this.totalChunks && this.processedChunks >= this.totalChunks;
        const allWorkersCompleted = this.completedWorkers.size >= this.processingWorkers.length;
        
        // 🔧 새로운 대안 완료 조건들
        const allChunksCompletedByTracking = this.chunkingComplete && this.allChunksCompleted();
        const noProgressFor10Seconds = this.lastProgressTime && (now - this.lastProgressTime) > 10000;
        const noChunkProcessedFor5Seconds = this.lastChunkProcessedTime && (now - this.lastChunkProcessedTime) > 5000;
        
        // 워커 유휴 상태 시간 추적
        if (allWorkersIdle && chunkingFinished && noMoreChunks) {
            if (!this.allWorkersIdleTime) {
                this.allWorkersIdleTime = now;
                console.log('💤 모든 워커 유휴 상태 시작 시점 기록');
            }
        } else {
            this.allWorkersIdleTime = null;
        }
        
        const allWorkersIdleFor5Seconds = this.allWorkersIdleTime && (now - this.allWorkersIdleTime) > 5000;
        
        // 🔧 하트비트 기반 완료 감지
        const heartbeatBasedCompletion = this.allWorkersReportIdle() && chunkingFinished && this.noProgressFor5Seconds();
        
        console.log(`🔍 워커 완료 상태 상세 확인:`);
        console.log(`   • 청킹 완료: ${chunkingFinished}`);
        console.log(`   • 바쁜 워커: ${this.busyWorkers.size}/${this.processingWorkers.length}`);
        console.log(`   • 대기 청크: ${this.pendingChunks?.length || 0}개`);
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
        } else if (allChunksProcessed && chunkingFinished && allWorkersIdle) {
            console.log('🎉 완료 조건D: 모든 청크 처리 + 워커 유휴');
            this.finalizeResults();
        } else if (chunkingFinished && allWorkersIdle && noMoreChunks && allWorkersIdleFor5Seconds) {
            console.log('🎉 완료 조건E: 5초간 워커 유휴 상태');
            this.finalizeResults();
        } else if (chunkingFinished && allWorkersIdle && noMoreChunks && noChunkProcessedFor5Seconds) {
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
    
    // 진행률 업데이트 (CLAUDE.md 사양 구현)
    updateProgress(stage, value) {
        this.progress[stage] = value;
        
        // CLAUDE.md 진행률 개선 방안 구현: 30% 청킹, 70% 처리
        if (stage === 'chunking') {
            // 청킹은 전체의 30%
            this.progress.overall = Math.min(30, value * 0.3);
        } else if (stage === 'processing') {
            // 처리는 전체의 70% (30%부터 시작)
            this.progress.overall = 30 + Math.min(70, value * 0.7);
        }
        
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
        
        // 추가 정보 계산 (CLAUDE.md 개선사항 포함)
        const progressInfo = {
            ...this.progress,
            busyWorkers: this.busyWorkers.size,
            totalWorkers: this.processingWorkers.length,
            processedChunks: this.processedChunks,
            totalChunks: this.totalChunks || 'unknown',
            completedWorkers: this.completedWorkers.size,
            processingSpeed: this.processingSpeed || 0,
            estimatedTimeRemaining: estimatedTimeRemaining,
            // 청킹 단계에서의 세분화된 상태
            isChunking: stage === 'chunking' && value < 100,
            isProcessing: (stage === 'processing' && value < 100) || (this.chunkingComplete && this.processedChunks < this.totalChunks && !this.isAnalysisComplete)
        };
        
        if (this.onProgressUpdate) {
            this.onProgressUpdate(progressInfo);
        }
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
            cpuCores: navigator.hardwareConcurrency || 'unknown'
        };
    }
}