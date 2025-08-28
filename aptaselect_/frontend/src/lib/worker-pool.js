// 워커 프로세스 풀 - CPU 코어 수에 맞춰 워커 관리
export class WorkerPool {
    constructor(workerScript, maxWorkers = null) {
        this.workerScript = workerScript;
        this.workers = [];
        this.availableWorkers = [];
        this.taskQueue = [];
        this.activeTasks = 0;
        
        // 최적화된 워커 수 계산
        this.maxWorkers = maxWorkers || this.calculateOptimalWorkers();
        
        console.log(`워커 풀 초기화: 최대 ${this.maxWorkers}개 워커 사용 (CPU: ${navigator.hardwareConcurrency || 'unknown'}, 메모리: ${navigator.deviceMemory || 'unknown'}GB)`);
        
        // 워커 초기화
        this.initializeWorkers();
    }

    // navigator.hardwareConcurrency와 deviceMemory를 고려한 최적 워커 수 계산
    calculateOptimalWorkers() {
        const cores = navigator.hardwareConcurrency || 4;
        const availableMemory = navigator.deviceMemory || 4; // GB 단위
        
        // 메모리 기반 동적 조정
        let memoryBasedLimit;
        if (availableMemory >= 8) {
            // 충분한 메모리: 모든 코어 활용
            memoryBasedLimit = cores;
        } else if (availableMemory >= 4) {
            // 중간 메모리: 코어 수의 75% 활용
            memoryBasedLimit = Math.ceil(cores * 0.75);
        } else {
            // 제한된 메모리: 최대 4개 워커로 제한
            memoryBasedLimit = Math.min(cores, 4);
        }
        
        // 최소 2개 워커 보장
        const optimalWorkers = Math.max(2, memoryBasedLimit);
        
        console.log(`워커 수 계산: CPU 코어=${cores}, 메모리=${availableMemory}GB, 최적 워커 수=${optimalWorkers}`);
        
        return optimalWorkers;
    }

    initializeWorkers() {
        for (let i = 0; i < this.maxWorkers; i++) {
            const worker = new Worker(this.workerScript, { type: 'module' });
            const workerInfo = {
                id: i,
                worker: worker,
                busy: false,
                currentTask: null
            };
            
            // 워커 메시지 처리
            worker.onmessage = (event) => {
                this.handleWorkerMessage(workerInfo, event);
            };
            
            worker.onerror = (error) => {
                console.error(`워커 ${i} 오류:`, error);
                this.handleWorkerError(workerInfo, error);
            };
            
            this.workers.push(workerInfo);
            this.availableWorkers.push(workerInfo);
        }
    }

    handleWorkerMessage(workerInfo, event) {
        const { type, data, taskId } = event.data;
        
        if (type === 'task_complete') {
            // 태스크 완료 처리
            this.handleTaskComplete(workerInfo, data, taskId);
        } else if (type === 'progress') {
            // 진행률 업데이트
            this.handleProgressUpdate(workerInfo, data, taskId);
        } else if (type === 'count_update') {
            // 카운트 업데이트
            this.handleCountUpdate(workerInfo, data, taskId);
        }
    }

    handleTaskComplete(workerInfo, result, taskId) {
        // 워커를 사용 가능 상태로 변경
        workerInfo.busy = false;
        this.availableWorkers.push(workerInfo);
        this.activeTasks--;
        
        // 태스크 결과 콜백 호출
        if (workerInfo.currentTask && workerInfo.currentTask.onComplete) {
            workerInfo.currentTask.onComplete(result, taskId);
        }
        
        workerInfo.currentTask = null;
        
        // 대기 중인 태스크 처리
        this.processNextTask();
    }

    handleProgressUpdate(workerInfo, progress, taskId) {
        if (workerInfo.currentTask && workerInfo.currentTask.onProgress) {
            workerInfo.currentTask.onProgress(progress, taskId, workerInfo.id);
        }
    }

    handleCountUpdate(workerInfo, counts, taskId) {
        if (workerInfo.currentTask && workerInfo.currentTask.onCountUpdate) {
            workerInfo.currentTask.onCountUpdate(counts, taskId, workerInfo.id);
        }
    }

    handleWorkerError(workerInfo, error) {
        console.error(`워커 ${workerInfo.id} 오류:`, error);
        
        // 워커 재시작
        workerInfo.worker.terminate();
        const newWorker = new Worker(this.workerScript, { type: 'module' });
        
        newWorker.onmessage = (event) => {
            this.handleWorkerMessage(workerInfo, event);
        };
        
        newWorker.onerror = (error) => {
            this.handleWorkerError(workerInfo, error);
        };
        
        workerInfo.worker = newWorker;
        workerInfo.busy = false;
        workerInfo.currentTask = null;
        
        // 사용 가능 워커 목록에 추가 (중복 방지)
        if (!this.availableWorkers.includes(workerInfo)) {
            this.availableWorkers.push(workerInfo);
        }
    }

    // 태스크 실행
    executeTask(taskData, callbacks = {}) {
        const task = {
            id: Date.now() + Math.random(),
            data: taskData,
            onComplete: callbacks.onComplete,
            onProgress: callbacks.onProgress,
            onCountUpdate: callbacks.onCountUpdate
        };
        
        this.taskQueue.push(task);
        this.processNextTask();
        
        return task.id;
    }

    processNextTask() {
        // 사용 가능한 워커와 대기 중인 태스크가 있는지 확인
        if (this.availableWorkers.length > 0 && this.taskQueue.length > 0) {
            const worker = this.availableWorkers.shift();
            const task = this.taskQueue.shift();
            
            worker.busy = true;
            worker.currentTask = task;
            this.activeTasks++;
            
            // 워커에 태스크 전송
            worker.worker.postMessage({
                type: 'process_chunk',
                taskId: task.id,
                data: task.data
            });
        }
    }

    // 모든 태스크 완료 대기
    async waitForAllTasks() {
        return new Promise((resolve) => {
            const checkCompletion = () => {
                if (this.activeTasks === 0 && this.taskQueue.length === 0) {
                    resolve();
                } else {
                    setTimeout(checkCompletion, 100);
                }
            };
            checkCompletion();
        });
    }

    // 워커 풀 종료
    terminate() {
        this.workers.forEach(workerInfo => {
            workerInfo.worker.terminate();
        });
        this.workers = [];
        this.availableWorkers = [];
        this.taskQueue = [];
    }

    // 런타임 중 워커 수 동적 조정
    adjustWorkerCount(newWorkerCount = null) {
        const targetCount = newWorkerCount || this.calculateOptimalWorkers();
        const currentCount = this.workers.length;
        
        if (targetCount === currentCount) {
            console.log(`워커 수 조정 불필요: 현재 ${currentCount}개`);
            return;
        }
        
        if (targetCount > currentCount) {
            // 워커 추가
            const workersToAdd = targetCount - currentCount;
            console.log(`워커 ${workersToAdd}개 추가: ${currentCount} → ${targetCount}`);
            
            for (let i = currentCount; i < targetCount; i++) {
                const worker = new Worker(this.workerScript, { type: 'module' });
                const workerInfo = {
                    id: i,
                    worker: worker,
                    busy: false,
                    currentTask: null
                };
                
                worker.onmessage = (event) => {
                    this.handleWorkerMessage(workerInfo, event);
                };
                
                worker.onerror = (error) => {
                    console.error(`워커 ${i} 오류:`, error);
                    this.handleWorkerError(workerInfo, error);
                };
                
                this.workers.push(workerInfo);
                this.availableWorkers.push(workerInfo);
            }
        } else {
            // 워커 제거 (사용 중이지 않은 워커만)
            const workersToRemove = currentCount - targetCount;
            console.log(`워커 ${workersToRemove}개 제거: ${currentCount} → ${targetCount}`);
            
            let removedCount = 0;
            for (let i = this.workers.length - 1; i >= 0 && removedCount < workersToRemove; i--) {
                const workerInfo = this.workers[i];
                if (!workerInfo.busy) {
                    workerInfo.worker.terminate();
                    this.workers.splice(i, 1);
                    
                    // availableWorkers 배열에서도 제거
                    const availableIndex = this.availableWorkers.indexOf(workerInfo);
                    if (availableIndex > -1) {
                        this.availableWorkers.splice(availableIndex, 1);
                    }
                    
                    removedCount++;
                }
            }
            
            if (removedCount < workersToRemove) {
                console.warn(`${workersToRemove - removedCount}개 워커가 사용 중이어서 제거하지 못했습니다.`);
            }
        }
        
        this.maxWorkers = targetCount;
        console.log(`워커 수 조정 완료: 현재 ${this.workers.length}개 워커 운영`);
    }

    // 시스템 성능 모니터링 기반 동적 조정 (향후 확장 가능)
    monitorAndAdjust() {
        // 메모리 사용률이나 CPU 사용률에 따라 워커 수를 동적으로 조정하는 로직
        // 현재는 기본 계산 로직으로 재조정
        this.adjustWorkerCount();
    }

    // 워커 풀 상태 정보
    getStatus() {
        return {
            totalWorkers: this.workers.length,
            availableWorkers: this.availableWorkers.length,
            busyWorkers: this.workers.filter(w => w.busy).length,
            queuedTasks: this.taskQueue.length,
            activeTasks: this.activeTasks,
            maxWorkers: this.maxWorkers,
            systemInfo: {
                cores: navigator.hardwareConcurrency || 'unknown',
                memory: navigator.deviceMemory || 'unknown'
            }
        };
    }
}