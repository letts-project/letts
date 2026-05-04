<?php
// intentional_fail — emit fd 3 fail event with structured details and exit
// with a non-zero code. Exercises the fd 3 fail wire format.

declare(strict_types=1);
require __DIR__ . '/../vendor/autoload.php';

use Letts\Mission;

$m = Mission::start();
$msg = (string) $m->input('message', 'boom');
$exit = (int) $m->input('exit_code', 2);

$m->fail($msg, exitCode: $exit, details: [
    'kind'        => 'dogfood',
    'arbitrary'   => ['nested' => true, 'count' => 3],
]);
