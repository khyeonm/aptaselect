import { run_fastq_join } from "./fastq-join";

self.addEventListener('message', function(e) {
    let sel_read1 = e.data.sel_read1;
    let sel_read2 = e.data.sel_read2;
    let s1_read1 = e.data.s1_read1;
    let s1_read2 = e.data.s1_read2;
    let s1_l = e.data.s1_l;
    let s2_read1 = e.data.s2_read1;
    let s2_read2 = e.data.s2_read2;
    let s2_l = e.data.s2_l;
    let read1 = e.data.read1[0];
    let read2 = e.data.read2[0];
    let is_short = e.data.is_short;
    aptaSelect(sel_read1, sel_read2, s1_read1, s1_read2, s1_l, s2_read1, s2_read2, s2_l, read1, read2, is_short);
}, false);

function filterFunc(seqs, p1, p2, m, l = 0, only_between = false) {
    const rtn = [];
    const p1Len = p1.length;
    const p2Len = p2.length;

    for (const seq of seqs) {
        const seqLen = seq.length;
        outerLoop:
        for (let i = 0; i <= seqLen - p1Len; i++) {
            if (m === 0 || countMismatches(seq, p1, i, m)) {
                for (let j = i + p1Len; j <= seqLen - p2Len; j++) {
                    if (m === 0 || countMismatches(seq, p2, j, m)) {
                        if (l === 0 || j - (i + p1Len) === l) {
                            if (only_between) {
                                rtn.push(seq.slice(i, j + p2Len));
                            } else {
                                rtn.push(seq);
                            }
                            break outerLoop;
                        }
                    }
                }
            }
        }
    }
    return rtn;
}

function countMismatches(seq, pattern, start, maxMismatches) {
    let mismatches = 0;
    for (let i = 0; i < pattern.length; i++) {
        if (pattern[i] !== seq[start + i]) {
            mismatches++;
            if (mismatches > maxMismatches) {
                return false;
            }
        }
    }
    return true;
}

function aptaSelect(sel_read1, sel_read2, s1_read1, s1_read2, s1_l, s2_read1, s2_read2, s2_l, read1, read2, is_short) {
    const files = [read1, read2];
    const pgCallback = function(pg) {
        postMessage({
                type: 'progress',
                data: pg
        });
    };
    const chunkCallback = function(chunk) {
        let sel = filterFunc(chunk, sel_read1, sel_read2, 1, 0, true);
        let s1 = filterFunc(sel, s1_read1, s1_read2, 1, s1_l);
        let s2 = filterFunc(s1, s2_read1, s2_read2, 1, s2_l);
        postMessage({
            type: 'chunk',
            data: {
                sel: sel,
                s1: s1,
                s2: s2
            }
        });
    };
    run_fastq_join(files, pgCallback, chunkCallback, is_short);
    self.postMessage('Processing complete');
}