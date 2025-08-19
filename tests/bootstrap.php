<?php

declare(strict_types=1);

use Symfony\Component\Dotenv\Dotenv;

require_once __DIR__ . '/../vendor/autoload.php';

// Load environment variables from .env file if it exists (for local development)
if (class_exists('Symfony\Component\Dotenv\Dotenv')) {
    $envFile = __DIR__ . '/../.env';
    if (file_exists($envFile)) {
        $dotenv = new Dotenv();
        $dotenv->load($envFile);
    }
}
