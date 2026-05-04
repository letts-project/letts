<?php
// big_io — take 3 named input files (small/medium/large), emit two output
// files (primary/aux) of different sizes. Exercises multi-role input and
// output staging delivery in one mission.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();

$sizes = [];
foreach (['small', 'medium', 'large'] as $role) {
    $sizes[$role] = $m->fileSize($role);
}

$primary = $m->outputPath('primary');
$aux     = $m->outputPath('aux');

// primary = concatenation of all three inputs.
$fp = fopen($primary, 'w');
foreach (['small', 'medium', 'large'] as $role) {
    $src = fopen($m->file($role), 'r');
    stream_copy_to_stream($src, $fp);
    fclose($src);
}
fclose($fp);

// aux = json summary.
file_put_contents($aux, json_encode(['sizes' => $sizes], JSON_PRETTY_PRINT));

$m->outputFile('primary');
$m->outputFile('aux');

$m->success([
    'inputs'  => $sizes,
    'primary' => filesize($primary),
    'aux'     => filesize($aux),
]);
