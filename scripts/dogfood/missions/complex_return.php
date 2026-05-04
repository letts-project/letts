<?php
// complex_return — emit a few progress events, then succeed with a deeply
// nested return value. Exercises RawMessage round-trip and `letts run`
// JSON output rendering.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();

$m->progress(0.25, 'phase 1');
$m->progress(0.50, 'phase 2');
$m->progress(0.75, 'phase 3');

$m->success([
    'meta' => [
        'version' => '1.2.3',
        'flags'   => ['fast' => true, 'beta' => false],
    ],
    'items' => [
        ['id' => 1, 'name' => 'alpha', 'tags' => ['a', 'b']],
        ['id' => 2, 'name' => 'beta',  'tags' => []],
        ['id' => 3, 'name' => 'gamma', 'tags' => ['c']],
    ],
    'totals' => ['ok' => 3, 'err' => 0],
]);
