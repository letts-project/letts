<?php
// slow_interruptible — loop N seconds with checkSignal between iterations.
// Exercises SIGTERM cooperative interrupt and timeout handling.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();
$seconds = (int) $m->input('seconds', 60);

for ($i = 1; $i <= $seconds; $i++) {
    $m->checkSignal();
    sleep(1);
    if ($i % 10 === 0) {
        $m->progress($i / $seconds, "elapsed {$i}s");
    }
}

$m->success(['slept_seconds' => $seconds]);
