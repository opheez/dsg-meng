New-Item -Name "results" -ItemType "directory"
$tmax = 48

$ps = (0.0, 0.1, 0.5, 1.0)
$ds = ("latch-free-mock", "latched", "simple-version", "two-phase-version")
foreach($d in $ds) { foreach ($p in $ps) { .\samples\ResizableListBench\bin\Release\net5.0\ResizableListBench.exe -t ($tmax / 2) -p $p -i 1000000 -d $d -l true -u ".\results\$d-p$p-latency"}}
foreach($d in $ds) { foreach ($p in $ps) { for ($t = 1; $t -lt ($tmax + 1); $t++) { .\samples\ResizableListBench\bin\Release\net5.0\ResizableListBench.exe -t $t -d $d -p $p -i 1000000 -u ".\results\$d-p$p-throughput.txt"}}}

$ms = ("latch-free", "epvs", "epvs-refresh", "latch")
$ps = (0, 1e-6, 1e-5, 1e-4, 1e-2, 1e-1, 1)
foreach ($m in $ms) { foreach ($p in $ps) { for ($t = 1; $t -lt ($tmax + 1); $t++) { .\samples\epvs\EpvsMicrobench\bin\Release\net5.0\EpvsMicrobench.exe -m $m -t $t -p $p -u ".\results\micro-$m-p$p-throughput.txt"}}}

foreach($m in $ms) { for ($e = $tmax; $e -lt (64 * $tmax); $e = $e * 2) { for ($t = 1; $t -lt ($tmax + 1); $t++) { .\samples\epvs\EpvsMicrobench\bin\Release\net5.0\EpvsMicrobench.exe -m $m -t $t -p 1e-4 -e $e  -u ".\results\micro-$m-e$e-throughput.txt"}}}

$chkpts= (500, 250, 100)
foreach($c in $chkpts) { for ($t = 1; $t -lt ($tmax + 1); $t++) {.\benchmark\bin\x64\Release\net5.0\FASTER.benchmark.exe -t $t --chkptms=$c --noaff=true --useepvs=false --output-file=".\results\orig-$c.txt"}}
foreach($c in $chkpts) { for ($t = 1; $t -lt ($tmax + 1); $t++) {.\benchmark\bin\x64\Release\net5.0\FASTER.benchmark.exe -t $t --chkptms=$c --noaff=true --useepvs=true --output-file=".\results\epvs-$c.txt"}}

$vs = ("none", "base", "epvs")
foreach ($v in $vs) {for ($t = 1; $t -lt ($tmax + 1); $t++) { .\benchmark\bin\x64\Release\net5.0\FASTER.benchmark.exe -t $t --noaff=true --validation=$v --output-file=".\results\validation-$v.txt"}}

