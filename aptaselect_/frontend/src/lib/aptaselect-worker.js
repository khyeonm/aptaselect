// AptaSelect 워커 - 시퀀스 조인 및 3단계 필터링 처리
import { FastqChunker } from './fastq-chunker.js';
import { run_fastq_join } from './fastq-join.js';

class AptaSelectProcessor {
    constructor() {
        this.chunker = new FastqChunker();
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
                console.log(`🔗 run_fastq_join 실행 시작 (${file1Data.length}개 레코드, is_short: ${is_short})`);
                
                // run_fastq_join 매개변수: (files, pgcallback, chunkcallback, reverse, mino, pctdiff)
                // is_short=true면 reverse=false (순서 그대로), is_short=false면 reverse=true (역순)
                const reverse = !is_short;
                run_fastq_join(tempFiles, pgCallback, chunkCallback, reverse, 6, 8);
                
                console.log(`✅ run_fastq_join 완료 (${joinedSequences.length}개 조인 시퀀스)`);
                resolve(joinedSequences);
                
            } catch (error) {
                console.error(`❌ run_fastq_join 오류:`, error);
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

    // Overlap을 찾아서 시퀀스를 조인하는 함수 (기존 fastq-join 로직 적용)
    findOverlapAndJoin(record1, record2) {
        const seq1 = record1.seq.split('');
        const seq2 = this.reverseComplement(record2.seq).split('');
        const qual1 = record1.qual.split('');
        const qual2 = record2.qual.split('').reverse();
        
        const minOverlap = 6;
        const maxDiff = 8;
        const maxOverlap = Math.min(seq1.length, seq2.length);
        
        let bestScore = Number.MAX_SAFE_INTEGER;
        let bestOverlap = -1;
        
        // Overlap 찾기
        for (let overlap = minOverlap; overlap <= maxOverlap; overlap++) {
            const mismatches = this.countMismatches(seq1, seq1.length - overlap, seq2, 0, overlap);
            const maxMismatches = Math.floor((maxDiff * overlap) / 100);
            
            if (mismatches <= maxMismatches) {
                const score = Math.floor((1000 * (mismatches * mismatches + 1)) / overlap);
                if (score < bestScore) {
                    bestScore = score;
                    bestOverlap = overlap;
                }
            }
        }
        
        if (bestOverlap > 0) {
            // Overlap 영역 병합
            for (let i = 0; i < bestOverlap; i++) {
                const pos1 = seq1.length - bestOverlap + i;
                const pos2 = i;
                
                if (seq1[pos1] === seq2[pos2]) {
                    // 동일한 경우 더 높은 quality 선택
                    qual1[pos1] = String.fromCharCode(Math.max(qual1[pos1].charCodeAt(0), qual2[pos2].charCodeAt(0)));
                } else {
                    // 다른 경우 더 높은 quality의 염기 선택
                    if (qual1[pos1].charCodeAt(0) > qual2[pos2].charCodeAt(0)) {
                        // seq1 선택
                        qual1[pos1] = String.fromCharCode(33 + Math.min(qual1[pos1].charCodeAt(0) - 33, 
                            Math.max(qual1[pos1].charCodeAt(0) - qual2[pos2].charCodeAt(0), 3)));
                    } else {
                        // seq2 선택
                        seq1[pos1] = seq2[pos2];
                        qual1[pos1] = String.fromCharCode(33 + Math.min(qual2[pos2].charCodeAt(0) - 33,
                            Math.max(qual2[pos2].charCodeAt(0) - qual1[pos1].charCodeAt(0), 3)));
                    }
                }
            }
            
            // 최종 시퀀스 생성
            const finalSeq = seq1.join('') + seq2.slice(bestOverlap).join('');
            const finalQual = qual1.join('') + qual2.slice(bestOverlap).join('');
            
            return {
                seq: finalSeq,
                qual: finalQual,
                id: record1.id
            };
        }
        
        // Overlap을 찾지 못한 경우 단순 연결
        return {
            seq: record1.seq + record2.seq,
            qual: record1.qual + record2.qual,
            id: record1.id
        };
    }

    // Reverse complement 생성
    reverseComplement(seq) {
        const complement = {
            'A': 'T', 'T': 'A', 'G': 'C', 'C': 'G',
            'a': 't', 't': 'a', 'g': 'c', 'c': 'g',
            'N': 'N', 'n': 'n'
        };
        
        return seq.split('').reverse().map(base => complement[base] || base).join('');
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

    // 시퀀스 필터링 함수 (기존 filterFunc 로직)
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

    // 청크 처리 메인 함수
    async processChunk(chunkData) {
        const {
            file1Records,
            file2Records,
            selectionParams,
            sort1Params,
            sort2Params,
            isShort
        } = chunkData;

        console.log(`청크 처리 시작: ${file1Records.length}개 레코드`);

        // 1단계: 시퀀스 조인 (CLAUDE.md 가이드라인: run_fastq_join 사용)
        const minRecords = Math.min(file1Records.length, file2Records.length);
        const trimmedFile1Records = file1Records.slice(0, minRecords);
        const trimmedFile2Records = file2Records.slice(0, minRecords);
        
        const rawJoinedSequences = await this.runFastqJoinOnChunk(
            trimmedFile1Records, 
            trimmedFile2Records, 
            isShort
        );
        
        // run_fastq_join 결과를 필터링에 맞는 형태로 변환
        const joinedSequences = rawJoinedSequences.map((seq, index) => ({
            seq: seq,
            qual: '',
            id: `joined_${index}`,
            validated: true,
            recordIndex: index
        }));

        console.log(`조인 완료: ${joinedSequences.length}개 시퀀스 (run_fastq_join 사용)`);

        // 2단계: 선택 필터링
        const selectedSequences = this.filterSequences(
            joinedSequences,
            selectionParams.read1,
            selectionParams.read2,
            1, // 1개 미스매치 허용
            0, // 길이 제한 없음
            true // 패턴 사이 영역만 추출
        );

        console.log(`선택 필터링 완료: ${selectedSequences.length}개 시퀀스`);

        // 3단계: 첫 번째 정렬 필터링
        const sorted1Sequences = this.filterSequences(
            selectedSequences,
            sort1Params.read1,
            sort1Params.read2,
            1, // 1개 미스매치 허용
            sort1Params.length,
            false // 전체 시퀀스 유지
        );

        console.log(`첫 번째 정렬 완료: ${sorted1Sequences.length}개 시퀀스`);

        // 4단계: 두 번째 정렬 필터링
        const sorted2Sequences = this.filterSequences(
            sorted1Sequences,
            sort2Params.read1,
            sort2Params.read2,
            1, // 1개 미스매치 허용
            sort2Params.length,
            false // 전체 시퀀스 유지
        );

        console.log(`두 번째 정렬 완료: ${sorted2Sequences.length}개 시퀀스`);

        // 중복 제거 및 카운팅
        const selectedCounts = this.countUniqueSequences(selectedSequences);
        const sorted1Counts = this.countUniqueSequences(sorted1Sequences);
        const sorted2Counts = this.countUniqueSequences(sorted2Sequences);

        return {
            // 카운트 데이터만 반환 (전체 서열 배열은 반환하지 않음)
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
}

// 워커 메인 로직
const processor = new AptaSelectProcessor();

self.addEventListener('message', async function(event) {
    const { type, taskId, data } = event.data;
    
    if (type === 'process_chunk') {
        try {
            // 진행률 0% 보고
            self.postMessage({
                type: 'progress',
                taskId: taskId,
                data: 0
            });

            // 청크 데이터 처리
            const result = await processor.processChunk(data);
            
            // 카운트 업데이트 보고
            self.postMessage({
                type: 'count_update',
                taskId: taskId,
                data: result.counts
            });
            
            // 진행률 100% 보고
            self.postMessage({
                type: 'progress',
                taskId: taskId,
                data: 100
            });
            
            // 작업 완료 보고
            self.postMessage({
                type: 'task_complete',
                taskId: taskId,
                data: result
            });
            
        } catch (error) {
            console.error('워커 처리 오류:', error);
            self.postMessage({
                type: 'error',
                taskId: taskId,
                error: error.message
            });
        }
    }
});