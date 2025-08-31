<script>
    import { onMount } from 'svelte';
    import { Input, Checkbox, Label, Button, Fileupload, Progressbar, Table, TableBody, TableBodyRow, TableBodyCell, TableHead, TableHeadCell } from 'flowbite-svelte';
    import { AptaSelectController } from '$lib/aptaselect-controller.js';

    let status = 0;
    let progress = 0;
    let progressMessage = ""; // 진행률 메시지
    let isChunking = false;   // 청킹 상태 플래그
    let processingMode = "";  // 처리 모드 (pull-based vs legacy)
    let validationStats = null; // 검증 통계

    let sel_read1, sel_read2, s1_read1, s1_read2, s1_l, s2_read1, s2_read2, s2_l, read1, read2;
    let fs;
    let is_short = true;

    let total_count = 0;  // 전체 조인된 시퀀스 수
    let sel_count = 0;
    let s1_count = 0;
    let s2_count = 0;
    let topSequences = {
        selected: [],
        sorted1: [],
        sorted2: []
    };

    let chart, chart_container;
    let chart_options;
    $: chart_options= {
        chart: {
            type: 'line',
            zoom: {
                enabled: false
            }
        },
        series: [
            {
                name: "Counts",
                data: [total_count, sel_count, s1_count, s2_count],
            },
        ],
        xaxis: {
            categories: ['Total Joined', 'Selected', 'Sorted 1', 'Sorted 2'],
        },
        grid: {
            row: {
                colors: ['#f3f3f3', 'transparent'],
                opacity: 0.5
            },
        },
        title: {
            text: 'Counts after selection and sorting',
            align: 'center'
        },
        stroke: {
            curve: 'straight'
        },
        dataLabels: {
            enabled: false
        },
    };

    function requestFileSystemAsync(type, size) {
        return new Promise((resolve, reject) => {
            window.requestFileSystem = window.requestFileSystem || window.webkitRequestFileSystem;
            window.requestFileSystem(type, size, resolve, reject);
        });
    }

    function createWriterAsync(fileEntry) {
        return new Promise((resolve, reject) => {
            fileEntry.createWriter(resolve, reject);
        });
    }

    function getFileAsync(fileEntry) {
        return new Promise((resolve, reject) => {
            fileEntry.file(resolve, reject);
        });
    }

    function getFileEntry(fileName, options) {
        return new Promise((resolve, reject) => {
            fs.root.getFile(fileName, options, resolve, reject);
        });
    }

    function writeToFile(w, s) {
        const b = new Blob([s], { type: 'text/plain' });
        return new Promise((resolve, reject) => {
            w.write(b);
            w.onwriteend = resolve;
            w.onerror = reject;
        });
    }

    function truncateFile(w) {
        return new Promise((resolve, reject) => {
            w.truncate(0);
            w.onwriteend = resolve;
            w.onerror = reject;
        });
    }

    let controller = null;

    async function run_analysis() {
        // 입력 검증
        if (!read1 || !read1[0] || !read2 || !read2[0]) {
            alert('Read 1과 Read 2 파일을 모두 선택해주세요.');
            return;
        }

        // 기본값 설정
        sel_read1 = sel_read1 || "CCACTTCTCCTTCCATCCTAAAC";
        sel_read2 = sel_read2 || "GAGTAGTTTGGAGGGTTGTCTG";
        s1_read1 = s1_read1 || "TCCTAAAC";
        s1_read2 = s1_read2 || "GAGTAGTT";
        s1_l = s1_l || 40;
        s2_read1 = s2_read1 || "TCTCTCTCTC";
        s2_read2 = s2_read2 || "GAGAGAGAGA";
        s2_l = s2_l || 20;

        console.log('분석 매개변수:', { sel_read1, sel_read2, s1_read1, s1_read2, s1_l, s2_read1, s2_read2, s2_l });

        try {
            status = 1;
            progress = 0;
            
            // 카운트 초기화
            total_count = 0;
            sel_count = 0;
            s1_count = 0;
            s2_count = 0;

            // 컨트롤러 초기화
            controller = new AptaSelectController();
            
            // 처리 모드 확인
            const controllerStatus = controller.getStatus();
            processingMode = controller.usePullBasedProcessing ? "Pull-based (CPU 최적화)" : "Legacy";
            console.log(`🚀 처리 모드: ${processingMode}`);
            
            // 콜백 설정 (CLAUDE.md 진행률 개선사항 구현)
            controller.onProgressUpdate = (progressInfo) => {
                progress = Math.round(progressInfo.overall);
                
                // 각 단계에 맞는 세분화된 진행률 표시 개선
                if (progressInfo.isChunking) {
                    progressMessage = `FASTQ 파일 분석 중... (${progressInfo.chunking.toFixed(1)}%)`;
                    if (progressInfo.totalChunks !== 'unknown') {
                        progressMessage += ` | 발견된 청크: ${progressInfo.processedChunks}`;
                    }
                } else if (progressInfo.isProcessing) {
                    // 처리 단계별 세분화된 메시지 표시
                    if (progressInfo.processing < 30) {
                        progressMessage = `시퀀스 조인 중... (${progressInfo.processing.toFixed(1)}%)`;
                    } else if (progressInfo.processing < 60) {
                        progressMessage = `선택 필터링 중... (${progressInfo.processing.toFixed(1)}%)`;
                    } else if (progressInfo.processing < 85) {
                        progressMessage = `1차 정렬 필터링 중... (${progressInfo.processing.toFixed(1)}%)`;
                    } else if (progressInfo.processing < 95) {
                        progressMessage = `2차 정렬 필터링 중... (${progressInfo.processing.toFixed(1)}%)`;
                    } else {
                        progressMessage = `카운트 집계 중... (${progressInfo.processing.toFixed(1)}%)`;
                    }
                    
                    // 처리 속도 표시
                    if (progressInfo.processingSpeed > 0) {
                        progressMessage += ` | 처리 속도: ${progressInfo.processingSpeed} 청크/초`;
                    }
                    
                    // 활성(바쁜) 워커 상태 - 올바른 값 사용
                    progressMessage += ` | 활성 워커: ${progressInfo.busyWorkers}/${progressInfo.totalWorkers}개`;
                    
                    // 청크 진행률
                    if (progressInfo.totalChunks !== 'unknown') {
                        progressMessage += ` | 청크: ${progressInfo.processedChunks}/${progressInfo.totalChunks}`;
                    }
                    
                    // 남은 시간 추정
                    if (progressInfo.estimatedTimeRemaining) {
                        const minutes = Math.floor(progressInfo.estimatedTimeRemaining / 60);
                        const seconds = progressInfo.estimatedTimeRemaining % 60;
                        if (minutes > 0) {
                            progressMessage += ` | 예상 완료: ${minutes}분 ${seconds}초`;
                        } else {
                            progressMessage += ` | 예상 완료: ${seconds}초`;
                        }
                    }
                } else {
                    progressMessage = `결과 집계 중... (${progress}%)`;
                }
                
                console.log(`${progressMessage} | 전체: ${progress}% (청킹: ${progressInfo.chunking.toFixed(1)}%, 처리: ${progressInfo.processing.toFixed(1)}%)`);
            };
            
            controller.onCountUpdate = (counts) => {
                total_count = counts.total || counts.joined || 0;  // 전체 조인된 시퀀스 수
                sel_count = counts.selected;
                s1_count = counts.sorted1;
                s2_count = counts.sorted2;
                
                // 검증 통계가 있으면 저장
                if (counts.validationStats) {
                    validationStats = counts.validationStats;
                }
                
                console.log('카운트 업데이트:', counts);
            };
            
            controller.onComplete = async (finalCounts) => {
                total_count = finalCounts.total || finalCounts.joined || 0;  // 전체 조인된 시퀀스 수
                sel_count = finalCounts.selected;
                s1_count = finalCounts.sorted1;
                s2_count = finalCounts.sorted2;
                topSequences = finalCounts.topSequences || {
                    selected: [],
                    sorted1: [],
                    sorted2: []
                };
                
                status = 2;
                progress = 100;
                
                // 차트 렌더링
                const ApexCharts = (await import('apexcharts')).default;
                chart = new ApexCharts(chart_container, chart_options);
                chart.render();
                
                console.log('분석 완료:', finalCounts);
            };
            
            controller.onError = (error) => {
                console.error('분석 오류:', error);
                alert(`분석 중 오류가 발생했습니다: ${error.message}`);
                status = 0;
            };
            
            // 분석 시작
            const files = [read1[0], read2[0]];
            const analysisParams = {
                sel_read1,
                sel_read2,
                s1_read1,
                s1_read2,
                s1_l: parseInt(s1_l),
                s2_read1,
                s2_read2,
                s2_l: parseInt(s2_l),
                is_short
            };
            
            await controller.startAnalysis(files, analysisParams);
            
        } catch (error) {
            console.error('분석 시작 오류:', error);
            alert(`분석을 시작할 수 없습니다: ${error.message}`);
            status = 0;
        }
    }

    async function downloadResults() {
        // 기존 FileSystem API 방식을 사용하지 않고 항상 새로운 카운트 기반 다운로드 사용
        await downloadResultsAlternative();
    }

    async function downloadResultsAlternative() {
        if (!controller) {
            alert('분석 결과가 없습니다.');
            return;
        }

        try {
            // 전체 시퀀스 데이터 조회
            const allSequences = await controller.getAllSequencesForDownload();
            
            console.log('다운로드할 시퀀스 데이터:', allSequences);
            console.log('데이터 타입 확인:', typeof allSequences[0], allSequences[0]);
            
            // 텍스트 파일 내용 생성 (메타데이터만, key=value 형식)
            let txtContent = [
                `## read1_file=${read1[0]?.name || 'N/A'}`,
                `## read2_file=${read2[0]?.name || 'N/A'}`,
                `## sel_read1=${sel_read1}`,
                `## sel_read2=${sel_read2}`,
                `## s1_read1=${s1_read1}`,
                `## s1_read2=${s1_read2}`,
                `## s1_length=${s1_l}`,
                `## s2_read1=${s2_read1}`,
                `## s2_read2=${s2_read2}`,
                `## s2_length=${s2_l}`,
                `## selected_count=${sel_count}`,
                `## s1_count=${s1_count}`,
                `## s2_count=${s2_count}`,
                '',
                '# sequence count',
                ''
            ];

            // 전체 시퀀스 데이터를 시퀀스 카운트 형식으로 추가
            allSequences.forEach(seq => {
                txtContent.push(`${seq.sequence} ${seq.count}`);
            });
            
            const txtString = txtContent.join('\n');

            // TXT 파일 다운로드
            const txtBlob = new Blob([txtString], { type: 'text/plain;charset=utf-8;' });
            const txtUrl = URL.createObjectURL(txtBlob);
            const txtLink = document.createElement('a');
            txtLink.href = txtUrl;
            txtLink.download = `aptaselect_results_${new Date().toISOString().split('T')[0]}.txt`;
            txtLink.click();
            URL.revokeObjectURL(txtUrl);

            console.log(`결과 다운로드 완료: ${allSequences.length}개 고유 시퀀스`);

        } catch (error) {
            console.error('다운로드 오류:', error);
            alert(`다운로드 중 오류가 발생했습니다: ${error.message}`);
        }
    }
</script>

<main class="p-4">
    <h1 class="text-3xl font-bold mb-4">AptaSelect</h1>
    {#if status === 0}
        <p>Enter sequences and upload read files to begin analysis.</p>
        <br>
        <section>
            <h2 class="text-xl font-semibold my-4">Read Files</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div class="mb-4">
                    <Label for="read1" class="mb-2">Read 1</Label>
                    <Fileupload id="read1" bind:files={read1} />
                </div>
                <div class="mb-4">
                    <Label for="read2" class="mb-2">Read 2</Label>
                    <Fileupload id="read2" bind:files={read2} />
                </div>
            </div>
            <Checkbox bind:checked={is_short} id="is_short">Library is shorter than the read length</Checkbox>
        </section>
        <section>
            <h2 class="text-xl font-semibold my-4">Sequences for selection of reads</h2>
            <div class="grid grid-cols-1 md:grid-cols-2 gap-4">
                <div class="mb-4">
                    <Label for="sel-read1" class="mb-2">Read 1</Label>
                    <Input type="text" id="sel-read1" bind:value={sel_read1} placeholder="Enter sequence for Read 1" />
                </div>
                <div class="mb-4">
                    <Label for="sel-read2" class="mb-2">Read 2</Label>
                    <Input type="text" id="sel-read2" bind:value={sel_read2} placeholder="Enter sequence for Read 2" />
                </div>
            </div>
        </section>
        <section>
            <h2 class="text-xl font-semibold my-4">Sequences for sort 1</h2>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div class="mb-4">
                    <Label for="s1-read1" class="mb-2">Read 1</Label>
                    <Input type="text" id="s1-read1" bind:value={s1_read1} placeholder="Enter sequence for Read 1" />
                </div>
                <div class="mb-4">
                    <Label for="s1-read2" class="mb-2">Read 2</Label>
                    <Input type="text" id="s1-read2" bind:value={s1_read2} placeholder="Enter sequence for Read 2" />
                </div>
                <div class="mb-4">
                    <Label for="s1-l" class="mb-2">Required length</Label>
                    <Input type="number" id="s1-l" bind:value={s1_l} placeholder="Enter length between reads" />
                </div>
            </div>
        </section>
        <section>
            <h2 class="text-xl font-semibold my-4">Sequences for sort 2</h2>
            <div class="grid grid-cols-1 md:grid-cols-3 gap-4">
                <div class="mb-4">
                    <Label for="s2-read1" class="mb-2">Read 1</Label>
                    <Input type="text" id="s2-read1" bind:value={s2_read1} placeholder="Enter sequence for Read 1" />
                </div>
                <div class="mb-4">
                    <Label for="s2-read2" class="mb-2">Read 2</Label>
                    <Input type="text" id="s2-read2" bind:value={s2_read2} placeholder="Enter sequence for Read 2" />
                </div>
                <div class="mb-4">
                    <Label for="s2-l" class="mb-2">Required length</Label>
                    <Input type="number" id="s2-l" bind:value={s2_l} placeholder="Enter length between reads" />
                </div>
            </div>
        </section>
        <br><br>
        <p align="center">
            <Button on:click={run_analysis} size="xl">
                Submit
            </Button>
        </p>
    {:else if status === 1}
        <p>{progressMessage || 'Analysis is running...'}</p>
        {#if processingMode}
            <p class="text-sm text-gray-600">처리 모드: {processingMode}</p>
        {/if}
        {#if validationStats}
            <p class="text-sm text-gray-600">Paired-read 검증: {validationStats.validRecords}/{validationStats.totalRecords} (오류율: {validationStats.errorRate})</p>
        {/if}
        <br><br>
        <Progressbar
            {progress}
            animate
            labelInside
            size="h-6"
            labelInsideClass="bg-blue-600 text-blue-100 text-base font-medium text-center p-1 leading-none rounded-full"
        />
    {:else if status === 2}
        <p>Analysis is complete!</p>
        {#if processingMode}
            <p class="text-sm text-gray-600">처리 모드: {processingMode}</p>
        {/if}
        {#if validationStats}
            <p class="text-sm text-gray-600">Paired-read 검증 결과: {validationStats.validRecords}/{validationStats.totalRecords} 성공 (오류율: {validationStats.errorRate})</p>
        {/if}
        <br>
        <p>Counts</p>
        <Table>
            <TableHead>
                <TableHeadCell>Total Joined</TableHeadCell>
                <TableHeadCell>Selected</TableHeadCell>
                <TableHeadCell>Sorted 1</TableHeadCell>
                <TableHeadCell>Sorted 2</TableHeadCell>
            </TableHead>
            <TableBody>
                <TableBodyRow>
                    <TableBodyCell>{total_count}</TableBodyCell>
                    <TableBodyCell>{sel_count}</TableBodyCell>
                    <TableBodyCell>{s1_count}</TableBodyCell>
                    <TableBodyCell>{s2_count}</TableBodyCell>
                </TableBodyRow>
            </TableBody>
        </Table>
        <br>
        <div bind:this={chart_container} />
        <br>
        
        <!-- 상위 시퀀스 결과 표시 -->
        {#if topSequences.sorted2 && topSequences.sorted2.length > 0}
            <h3 class="text-xl font-semibold mb-4">상위 10개 시퀀스 (카운트 순)</h3>
            <Table>
                <TableHead>
                    <TableHeadCell>순위</TableHeadCell>
                    <TableHeadCell>시퀀스</TableHeadCell>
                    <TableHeadCell>카운트</TableHeadCell>
                </TableHead>
                <TableBody>
                    {#each topSequences.sorted2.slice(0, 10) as seq, index}
                        <TableBodyRow>
                            <TableBodyCell>{index + 1}</TableBodyCell>
                            <TableBodyCell class="font-mono text-sm break-all max-w-md">{seq.sequence}</TableBodyCell>
                            <TableBodyCell>{seq.count}</TableBodyCell>
                        </TableBodyRow>
                    {/each}
                </TableBody>
            </Table>
            <br>
        {/if}
        
        <Button size="xl" on:click={downloadResults}>
            Download Results
        </Button>
    {/if}
</main>