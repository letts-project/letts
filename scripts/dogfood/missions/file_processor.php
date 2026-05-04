<?php
// file_processor — read input file role `in`, uppercase its content, write
// to output role `out`. Exercises input-files staging delivery and output
// staging registration.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();

$inPath = $m->file('in');
$content = file_get_contents($inPath);
if ($content === false) {
    $m->fail("could not read input file role=in at $inPath");
}
$upper = strtoupper($content);

file_put_contents($m->outputPath('out'), $upper);
$m->outputFile('out');

$m->success(['in_bytes' => strlen($content), 'out_bytes' => strlen($upper)]);
