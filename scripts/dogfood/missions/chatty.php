<?php
// chatty — write N timestamped lines to stdout at intervals. Used by
// scenario 08 to verify live log streaming through the daemon's output
// file and the letts CLI follow-tail.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();
$n = (int) ($m->input('lines', 10));
$delayMs = (int) ($m->input('delay_ms', 200));

for ($i = 1; $i <= $n; $i++) {
    echo "[chatty $i/$n] " . sprintf('%.6f', microtime(true)) . "\n";
    // Default PHP stdout is line-buffered when attached to a tty but
    // fully-buffered when redirected (which dugdale does — stdout goes to
    // a file). Force a flush per line so the daemon's tail sees each line
    // immediately rather than at process exit.
    if (function_exists('fflush')) {
        fflush(STDOUT);
    } else {
        flush();
    }
    usleep($delayMs * 1000);
}

$m->success(['lines' => $n]);
