<?php

namespace App;

final class Parser
{
    private const int READ_CHUNK_SIZE = 16_388_608;
    private const int WORKERS = 2;
    private const int DISCOVER_SIZE = 16_388_608;

    public function parse(string $inputPath, string $outputPath): void
    {
        ini_set('memory_limit', '1280M');

        $fileSize = filesize($inputPath);

        $chunkSize = (int) ($fileSize / self::WORKERS);
        $boundaries = [0];
        $handle = fopen($inputPath, 'rb');
        for ($i = 1; $i < self::WORKERS; $i++) {
            fseek($handle, $i * $chunkSize);
            fgets($handle);
            $boundaries[] = ftell($handle);
        }
        fclose($handle);
        $boundaries[] = $fileSize;

        [$pathIds, $paths, $pathCount, $dateIds, $dates, $dateCount] = $this->discover($inputPath, $fileSize);

        $tmpDir = sys_get_temp_dir();
        $myPid = getmypid();
        $tmpFiles = [];
        $pids = [];

        for ($i = 0; $i < self::WORKERS - 1; $i++) {
            $tmpFile = $tmpDir . '/parse_' . $myPid . '_' . $i;
            $tmpFiles[$i] = $tmpFile;
            $pid = pcntl_fork();

            if ($pid === 0) {
                $data = $this->processChunk(
                    $inputPath, $boundaries[$i], $boundaries[$i + 1],
                    $pathIds, $dateIds, $pathCount, $dateCount,
                );
                file_put_contents($tmpFile, pack('V*', ...$data));
                exit(0);
            }

            $pids[$i] = $pid;
        }

        $mergedCounts = $this->processChunk(
            $inputPath, $boundaries[self::WORKERS - 1], $boundaries[self::WORKERS],
            $pathIds, $dateIds, $pathCount, $dateCount,
        );

        foreach ($pids as $pid) {
            pcntl_waitpid($pid, $status);
        }

        foreach ($tmpFiles as $tmpFile) {
            $wCounts = unpack('V*', (string) file_get_contents($tmpFile));
            unlink($tmpFile);
            $j = 0;
            foreach ($wCounts as $v) {
                $mergedCounts[$j++] += $v;
            }
        }

        $this->writeOutput($outputPath, $paths, $dates, $mergedCounts, $dateCount);
    }

    private function discover(string $inputPath, int $fileSize): array
    {
        $dateIds = [];
        $dates = [];
        $dateCount = 0;

        for ($y = 2020; $y <= 2026; $y++) {
            for ($m = 1; $m <= 12; $m++) {
                $daysInMonth = match ($m) {
                    2 => ($y % 4 === 0) ? 29 : 28,
                    4, 6, 9, 11 => 30,
                    default => 31,
                };
                for ($d = 1; $d <= $daysInMonth; $d++) {
                    $date = $y . '-' . ($m < 10 ? '0' : '') . $m . '-' . ($d < 10 ? '0' : '') . $d;
                    $dateIds[$date] = $dateCount;
                    $dates[$dateCount] = $date;
                    $dateCount++;
                }
            }
        }

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        $chunk = fread($handle, min($fileSize, self::DISCOVER_SIZE));
        fclose($handle);

        $lastNl = strrpos($chunk, "\n");
        $pathIds = [];
        $paths = [];
        $pathCount = 0;
        $pos = 0;

        while ($pos < $lastNl) {
            $nlPos = strpos($chunk, "\n", $pos + 55);
            $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);

            if (!isset($pathIds[$path])) {
                $pathIds[$path] = $pathCount;
                $paths[$pathCount] = $path;
                $pathCount++;
            }

            $pos = $nlPos + 1;
        }

        return [$pathIds, $paths, $pathCount, $dateIds, $dates, $dateCount];
    }

    private function processChunk(
        string $inputPath, int $start, int $end,
        array $pathIds, array $dateIds,
        int $pathCount, int $dateCount,
    ): array {
        $stride = $dateCount;
        $counts = array_fill(0, $pathCount * $stride, 0);

        $handle = fopen($inputPath, 'rb');
        stream_set_read_buffer($handle, 0);
        fseek($handle, $start);
        $remaining = $end - $start;

        while ($remaining > 0) {
            $chunk = fread($handle, $remaining > self::READ_CHUNK_SIZE ? self::READ_CHUNK_SIZE : $remaining);
            $chunkLen = strlen($chunk);
            $remaining -= $chunkLen;

            $lastNl = strrpos($chunk, "\n");

            if ($lastNl < ($chunkLen - 1)) {
                $excess = $chunkLen - $lastNl - 1;
                fseek($handle, -$excess, SEEK_CUR);
                $remaining += $excess;
            }

            $pos = 0;
            while ($pos < $lastNl) {
                $nlPos = strpos($chunk, "\n", $pos + 55);

                $path = substr($chunk, $pos + 25, $nlPos - $pos - 51);
                $date = substr($chunk, $nlPos - 25, 10);

                $pathId = $pathIds[$path] ?? -1;
                if ($pathId === -1) {
                    $pathId = $pathCount;
                    $pathIds[$path] = $pathId;
                    for ($j = 0; $j < $stride; $j++) {
                        $counts[] = 0;
                    }
                    $pathCount++;
                }

                $counts[($pathId * $stride) + $dateIds[$date]]++;
                $pos = $nlPos + 1;
            }
        }

        fclose($handle);
        return $counts;
    }

    private function writeOutput(
        string $outputPath, array $paths, array $dates,
        array $counts, int $dateCount,
    ): void {
        $sortedDates = $dates;
        asort($sortedDates);
        $orderedDateIds = array_keys($sortedDates);

        $out = fopen($outputPath, 'wb');
        stream_set_write_buffer($out, 1_048_576);
        fwrite($out, '{');

        $firstPath = true;
        foreach ($paths as $pathId => $path) {
            $pathBuffer = $firstPath ? '' : ',';
            $firstPath = false;
            $pathBuffer .= "\n    \"\\/blog\\/{$path}\": {";

            $entries = [];
            $base = $pathId * $dateCount;

            foreach ($orderedDateIds as $dateId) {
                $count = $counts[$base + $dateId];
                if ($count === 0) continue;
                $entries[] = "        \"{$dates[$dateId]}\": {$count}";
            }

            $pathBuffer .= "\n" . implode(",\n", $entries) . "\n    }";
            fwrite($out, $pathBuffer);
        }

        fwrite($out, "\n}");
        fclose($out);
    }
}
