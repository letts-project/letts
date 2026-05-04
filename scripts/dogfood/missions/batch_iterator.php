<?php
// batch_iterator — loop N=20 emitting progress events with small sleeps.
// Exercises progress event stream rate, ordering, and final success.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();
$n = (int) ($m->input('n', 20));
$delayMs = (int) ($m->input('delay_ms', 50));

for ($i = 1; $i <= $n; $i++) {
    $m->checkSignal();
    $m->progress($i / $n, "step $i/$n");
    usleep($delayMs * 1000);
}

$m->success(['iterations' => $n]);
