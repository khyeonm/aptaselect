// AptaSelect 메인 컨트롤러 - Pull 기반 파이프라인 지원
import { FastqChunker } from './fastq-chunker.js';
import { WorkerPool } from './worker-pool.js';
import { DuckDBProcessor } from './duckdb-processor.js';
import { PullBasedAptaSelectController } from './pull-based-aptaselect-controller.js';

export class AptaSelectController {
    constructor() {
        // 기존 방식과 새로운 Pull 기반 방식 지원
        this.usePullBasedProcessing = true; // 기본적으로 Pull 기반 사용
        
        // 기존 구성 요소들 (호환성 유지)
        this.chunker = new FastqChunker();
        this.workerPool = null;
        this.duckDB = new DuckDBProcessor();
        
        // Pull 기반 컨트롤러
        this.pullBasedController = null;
        
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
        this.totalChunks = 0;
        this.processedChunks = 0;
        this.workerCounts = new Map(); // 워커별 카운트 추적
    }

    // 분석 시작 - Pull 기반 또는 기존 방식 선택
    async startAnalysis(files, analysisParams) {
        if (this.usePullBasedProcessing) {
            return this.startPullBasedAnalysis(files, analysisParams);
        } else {
            return this.startLegacyAnalysis(files, analysisParams);
        }
    }
    
    // Pull 기반 분석 시작
    async startPullBasedAnalysis(files, analysisParams) {
        try {
            console.log('🚀 Pull 기반 AptaSelect 분석 시작');
            
            // Pull 기반 컨트롤러 초기화
            this.pullBasedController = new PullBasedAptaSelectController();
            
            // 콜백 설정
            this.pullBasedController.onProgressUpdate = (progressInfo) => {
                this.progress = progressInfo;
                if (this.onProgressUpdate) {
                    this.onProgressUpdate(progressInfo);
                }
            };
            
            this.pullBasedController.onCountUpdate = (counts) => {
                this.counts = counts;
                if (this.onCountUpdate) {
                    this.onCountUpdate(counts);
                }
            };
            
            this.pullBasedController.onComplete = (finalCounts) => {
                this.counts = finalCounts;
                this.isProcessing = false;
                console.log('🎉 Pull 기반 분석 완료:', finalCounts);
                
                if (this.onComplete) {
                    this.onComplete(finalCounts);
                }
            };
            
            this.pullBasedController.onError = (error) => {
                this.isProcessing = false;
                console.error('Pull 기반 분석 오류:', error);
                if (this.onError) {
                    this.onError(error);
                }
            };
            
            // 분석 시작
            this.isProcessing = true;
            this.pullBasedController.files = files;
            this.pullBasedController.analysisParams = analysisParams;
            
            await this.pullBasedController.startOptimizedAnalysis(files, analysisParams);
            
        } catch (error) {
            this.isProcessing = false;
            console.error('Pull 기반 분석 시작 오류:', error);
            if (this.onError) {
                this.onError(error);
            }
        }
    }
    
    // 기존 방식 분석 (호환성 유지)
    async startLegacyAnalysis(files, analysisParams) {
        try {
            this.isProcessing = true;
            this.resetCounts();
            
            console.log('기존 방식 AptaSelect 분석 시작');
            
            // 1단계: 파일 청킹 (진행률 콜백 추가)
            this.updateProgress('chunking', 0);
            const chunkInfo = await this.chunker.getChunkPositions(files, (stage, progress) => {
                this.updateProgress(stage, progress);
            });
            this.totalChunks = chunkInfo.totalChunks;
            this.updateProgress('chunking', 50); // 청킹 완료
            
            console.log(`청킹 완료: ${this.totalChunks}개 청크, 총 ${chunkInfo.totalRecords}개 레코드`);
            
            // 2단계: DuckDB 초기화
            await this.duckDB.initialize();
            await this.duckDB.clearTables();
            
            // 3단계: 워커 풀 초기화
            this.workerPool = new WorkerPool(
                new URL('./aptaselect-worker.js', import.meta.url)
            );
            
            console.log('워커 풀 초기화 완료');
            
            // 4단계: 청크별 처리
            this.updateProgress('processing', 0);
            await this.processChunks(files, chunkInfo, analysisParams);
            
            // 5단계: 최종 집계
            await this.finalizeResults();
            
            this.updateProgress('overall', 100);
            
            if (this.onComplete) {
                this.onComplete(this.counts);
            }
            
            console.log('분석 완료:', this.counts);
            
        } catch (error) {
            console.error('분석 오류:', error);
            if (this.onError) {
                this.onError(error);
            }
        } finally {
            this.isProcessing = false;
            if (this.workerPool) {
                this.workerPool.terminate();
            }
        }
    }

    // 청크별 처리
    async processChunks(files, chunkInfo, analysisParams) {
        const { chunkPositions } = chunkInfo;
        const promises = [];
        
        // 각 청크를 워커로 처리
        for (let chunkId = 0; chunkId < this.totalChunks; chunkId++) {
            const chunk1Info = chunkPositions[0][chunkId];
            const chunk2Info = chunkPositions[1][chunkId];
            
            const taskPromise = this.processChunk(
                files, chunk1Info, chunk2Info, chunkId, analysisParams
            );
            
            promises.push(taskPromise);
        }
        
        // 모든 청크 처리 완료 대기
        await Promise.all(promises);
        console.log('모든 청크 처리 완료');
    }

    // 개별 청크 처리
    async processChunk(files, chunk1Info, chunk2Info, chunkId, analysisParams) {
        return new Promise((resolve, reject) => {
            // 청크 데이터 읽기
            Promise.all([
                this.chunker.readChunkData(files[0], chunk1Info),
                this.chunker.readChunkData(files[1], chunk2Info)
            ]).then(([file1Records, file2Records]) => {
                
                const taskData = {
                    file1Records,
                    file2Records,
                    selectionParams: {
                        read1: analysisParams.sel_read1,
                        read2: analysisParams.sel_read2
                    },
                    sort1Params: {
                        read1: analysisParams.s1_read1,
                        read2: analysisParams.s1_read2,
                        length: analysisParams.s1_l
                    },
                    sort2Params: {
                        read1: analysisParams.s2_read1,
                        read2: analysisParams.s2_read2,
                        length: analysisParams.s2_l
                    },
                    isShort: analysisParams.is_short,
                    chunkId: chunkId
                };
                
                // 워커에 태스크 전송
                this.workerPool.executeTask(taskData, {
                    onComplete: async (result, taskId) => {
                        await this.handleChunkComplete(result, chunkId);
                        resolve();
                    },
                    onProgress: (progress, taskId, workerId) => {
                        this.handleChunkProgress(progress, chunkId, workerId);
                    },
                    onCountUpdate: (counts, taskId, workerId) => {
                        this.handleCountUpdate(counts, chunkId, workerId);
                    }
                });
                
            }).catch(reject);
        });
    }

    // 청크 처리 완료 핸들러
    async handleChunkComplete(result, chunkId) {
        try {
            // 워커에서 받은 카운트 데이터 집계
            this.aggregateSequenceCounts('selected', result.selectedCounts || []);
            this.aggregateSequenceCounts('sorted1', result.sorted1Counts || []);
            this.aggregateSequenceCounts('sorted2', result.sorted2Counts || []);
            
            // 전체 카운트 업데이트
            this.counts.selected += result.counts.selected;
            this.counts.sorted1 += result.counts.sorted1;
            this.counts.sorted2 += result.counts.sorted2;
            
            this.processedChunks++;
            const processingProgress = (this.processedChunks / this.totalChunks) * 100;
            this.updateProgress('processing', processingProgress);
            
            // 카운트 업데이트 콜백 호출
            if (this.onCountUpdate) {
                this.onCountUpdate(this.counts);
            }
            
            console.log(`청크 ${chunkId} 처리 완료 (${this.processedChunks}/${this.totalChunks})`);
            
        } catch (error) {
            console.error(`청크 ${chunkId} 완료 처리 오류:`, error);
        }
    }

    // 시퀀스 카운트 집계
    aggregateSequenceCounts(type, newCounts) {
        const targetMap = this.sequenceCounts[type];
        
        console.log(`${type} 집계 중: ${newCounts.length}개 고유 시퀀스 받음`);
        console.log(`첫 3개 샘플:`, newCounts.slice(0, 3));
        
        for (const { sequence, count } of newCounts) {
            if (targetMap.has(sequence)) {
                targetMap.set(sequence, targetMap.get(sequence) + count);
            } else {
                targetMap.set(sequence, count);
            }
        }
        
        console.log(`${type} 집계 완료: 현재 ${targetMap.size}개 고유 시퀀스`);
    }

    // 청크 진행률 핸들러
    handleChunkProgress(progress, chunkId, workerId) {
        // 개별 청크 진행률은 전체 진행률에 반영하지 않음 (너무 복잡함)
        console.log(`청크 ${chunkId} (워커 ${workerId}): ${progress}%`);
    }

    // 카운트 업데이트 핸들러
    handleCountUpdate(counts, chunkId, workerId) {
        // 워커별 카운트 저장
        const workerKey = `${workerId}_${chunkId}`;
        this.workerCounts.set(workerKey, counts);
        
        // 전체 카운트 재계산
        this.aggregateCounts();
        
        if (this.onCountUpdate) {
            this.onCountUpdate(this.counts);
        }
    }

    // 전체 카운트 집계
    aggregateCounts() {
        const aggregated = {
            total: 0,
            selected: 0,
            sorted1: 0,
            sorted2: 0
        };
        
        // 모든 워커의 카운트 합산
        for (const counts of this.workerCounts.values()) {
            aggregated.selected += counts.selected || 0;
            aggregated.sorted1 += counts.sorted1 || 0;
            aggregated.sorted2 += counts.sorted2 || 0;
            aggregated.total += counts.joined || 0;
        }
        
        this.counts = aggregated;
    }

    // 최종 결과 집계
    async finalizeResults() {
        try {
            // 메모리에서 상위 시퀀스 생성
            const topSequences = {
                selected: this.getTopSequencesFromMap('selected', 10),
                sorted1: this.getTopSequencesFromMap('sorted1', 10),
                sorted2: this.getTopSequencesFromMap('sorted2', 10)
            };
            
            this.counts.topSequences = topSequences;
            
            console.log('최종 집계 완료:', this.counts);
            console.log('상위 시퀀스 개수:', {
                selected: topSequences.selected.length,
                sorted1: topSequences.sorted1.length,
                sorted2: topSequences.sorted2.length
            });
            
        } catch (error) {
            console.error('최종 집계 오류:', error);
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

    // 진행률 업데이트
    updateProgress(stage, value) {
        this.progress[stage] = value;
        
        // overall 계산 시 청킹 진행률도 포함
        if (stage === 'chunking' && value <= 50) {
            this.progress.overall = value; // 청킹 단계에서는 청킹 진행률을 overall에 직접 반영
        } else {
            this.progress.overall = (this.progress.chunking + this.progress.processing) / 2;
        }
        
        if (this.onProgressUpdate) {
            this.onProgressUpdate(this.progress);
        }
    }

    // 카운트 초기화
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
        this.workerCounts.clear();
    }

    // 분석 중단
    async stopAnalysis() {
        this.isProcessing = false;
        
        if (this.usePullBasedProcessing && this.pullBasedController) {
            await this.pullBasedController.stopAnalysis();
            this.pullBasedController = null;
        } else if (this.workerPool) {
            this.workerPool.terminate();
            this.workerPool = null;
        }
        
        console.log('분석 중단됨');
    }

    // 전체 시퀀스 데이터 조회 (다운로드용)
    async getAllSequencesForDownload() {
        try {
            if (this.usePullBasedProcessing && this.pullBasedController) {
                return await this.pullBasedController.getAllSequencesForDownload();
            } else {
                // 기존 방식: 메모리에서 전체 시퀀스와 카운트 데이터 조회
                const allSequences = this.getTopSequencesFromMap('sorted2', -1); // -1은 전체를 의미
                
                console.log(`다운로드용 전체 시퀀스 조회 완료: ${allSequences.length}개`);
                console.log('첫 5개 시퀀스 샘플:', allSequences.slice(0, 5));
                
                return allSequences;
            }

        } catch (error) {
            console.error('전체 시퀀스 조회 오류:', error);
            throw error;
        }
    }

    // 상태 조회
    getStatus() {
        if (this.usePullBasedProcessing && this.pullBasedController) {
            return this.pullBasedController.getStatus();
        } else {
            return {
                isProcessing: this.isProcessing,
                progress: this.progress,
                counts: this.counts,
                totalChunks: this.totalChunks,
                processedChunks: this.processedChunks,
                workerPoolStatus: this.workerPool ? this.workerPool.getStatus() : null,
                processingMode: 'legacy'
            };
        }
    }
}