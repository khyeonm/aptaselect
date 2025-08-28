// FASTQ 파일 청킹 함수 - 10000개 레코드씩 위치 정보만 저장
import { jbfilereader } from "./jbfilereader";

export class FastqChunker {
    constructor() {
        this.CHUNK_SIZE = 10000; // 10000개 레코드씩 청킹
    }

    // FASTQ 파일들의 청킹 위치 정보를 추출
    async getChunkPositions(files, progressCallback = null) {
        const readers = [];
        const chunkPositions = [[], []]; // [file1_positions, file2_positions]
        
        // 파일 리더 초기화
        for (let i = 0; i < 2; i++) {
            const file = files[i];
            const isGzipped = file.name.endsWith('.gz');
            if (isGzipped) {
                throw new Error('압축된 .gz 파일은 지원하지 않습니다. 압축되지 않은 .fq 파일만 사용해주세요.');
            }
            readers[i] = new jbfilereader(file, false);
        }

        let recordCount = 0;
        let currentChunk = 0;
        const PROGRESS_UPDATE_INTERVAL = 50000; // 5만 레코드마다 업데이트
        const totalFileSize = files[0].size + files[1].size;
        
        // 각 파일의 시작 위치 기록
        chunkPositions[0].push({ 
            chunkId: currentChunk, 
            startPos: 0, 
            recordCount: 0 
        });
        chunkPositions[1].push({ 
            chunkId: currentChunk, 
            startPos: 0, 
            recordCount: 0 
        });

        try {
            while (true) {
                let eof = false;
                
                // 두 파일에서 동시에 한 레코드씩 읽기 (4줄 = 1 레코드)
                for (let fileIdx = 0; fileIdx < 2; fileIdx++) {
                    const reader = readers[fileIdx];
                    
                    // FASTQ 레코드 4줄 읽기 (비동기)
                    for (let lineType = 0; lineType < 4; lineType++) {
                        const line = await new Promise((resolve) => {
                            reader.readline(resolve);
                        });
                        
                        if (line === "") {
                            eof = true;
                            break;
                        }
                        
                        // 첫 번째 줄(@로 시작)인지 확인
                        if (lineType === 0 && !line.startsWith('@')) {
                            throw new Error('FASTQ 형식이 아닙니다!');
                        }
                    }
                    
                    if (eof) break;
                }
                
                if (eof) break;
                
                recordCount++;
                
                // 진행률 업데이트 (5만 레코드마다)
                if (recordCount % PROGRESS_UPDATE_INTERVAL === 0 && progressCallback) {
                    // 파일 크기 기반 대략적인 진행률 계산
                    const currentPos = readers[0].fpos + readers[1].fpos;
                    const fileProgress = (currentPos / totalFileSize) * 50; // 청킹은 전체의 50%
                    
                    progressCallback('chunking', Math.min(fileProgress, 50));
                    
                    // UI 응답성을 위해 메인 스레드에 제어권 양보
                    await new Promise(resolve => setTimeout(resolve, 0));
                }
                
                // 10000개 레코드마다 청킹 위치 기록
                if (recordCount % this.CHUNK_SIZE === 0) {
                    currentChunk++;
                    
                    for (let fileIdx = 0; fileIdx < 2; fileIdx++) {
                        chunkPositions[fileIdx].push({
                            chunkId: currentChunk,
                            startPos: readers[fileIdx].fpos,
                            recordCount: recordCount
                        });
                    }
                }
            }
            
            // 마지막 청크 정보 추가 (남은 레코드가 있는 경우)
            if (recordCount % this.CHUNK_SIZE !== 0) {
                for (let fileIdx = 0; fileIdx < 2; fileIdx++) {
                    chunkPositions[fileIdx][chunkPositions[fileIdx].length - 1].endPos = readers[fileIdx].filesize;
                    chunkPositions[fileIdx][chunkPositions[fileIdx].length - 1].totalRecords = recordCount;
                }
            }
            
        } catch (error) {
            throw new Error(`FASTQ 파일 처리 중 오류: ${error.message}`);
        }

        return {
            chunkPositions,
            totalRecords: recordCount,
            totalChunks: currentChunk + 1
        };
    }

    // 특정 청크의 데이터를 읽어오는 함수
    async readChunkData(file, chunkInfo) {
        console.log(`청크 데이터 읽기 시작:`, chunkInfo);
        
        const reader = new jbfilereader(file, false);
        const records = [];
        
        // 첫 번째 청크는 처음부터 읽기
        if (chunkInfo.chunkId === 0) {
            console.log('첫 번째 청크 - 처음부터 읽기');
        } else {
            console.log(`청크 ${chunkInfo.chunkId} - 위치 설정 시도:`, chunkInfo.startPos);
            // 다른 청크의 경우 위치 설정이 복잡하므로 일단 처음부터 읽고 스킵
        }
        
        let recordsToRead = this.CHUNK_SIZE;
        if (chunkInfo.totalRecords) {
            recordsToRead = Math.min(this.CHUNK_SIZE, 
                chunkInfo.totalRecords - chunkInfo.recordCount);
        }
        
        console.log(`읽을 레코드 수: ${recordsToRead}`);
        
        // 다른 청크인 경우 이전 레코드들을 스킵
        if (chunkInfo.chunkId > 0) {
            const skipRecords = chunkInfo.recordCount;
            console.log(`${skipRecords}개 레코드 스킵`);
            
            for (let i = 0; i < skipRecords; i++) {
                await new Promise((resolve) => reader.readline(resolve)); // id
                await new Promise((resolve) => reader.readline(resolve)); // seq  
                await new Promise((resolve) => reader.readline(resolve)); // plus
                await new Promise((resolve) => reader.readline(resolve)); // qual
            }
        }
        
        // 실제 레코드 읽기
        for (let i = 0; i < recordsToRead; i++) {
            const record = {
                id: await new Promise((resolve) => reader.readline(resolve)),
                seq: await new Promise((resolve) => reader.readline(resolve)),
                plus: await new Promise((resolve) => reader.readline(resolve)),
                qual: await new Promise((resolve) => reader.readline(resolve))
            };
            
            console.log(`레코드 ${i}:`, record.id ? record.id.substring(0, 20) + '...' : 'empty');
            
            if (record.id === "" || !record.id.startsWith('@')) {
                console.log(`레코드 읽기 종료 at ${i}`);
                break;
            }
            
            records.push(record);
        }
        
        console.log(`청크 데이터 읽기 완료: ${records.length}개 레코드`);
        return records;
    }
}