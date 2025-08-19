FROM php:8.2

RUN apt-get update -q \
  && apt-get install git unzip \
  -y --no-install-recommends

COPY docker/php.ini /usr/local/etc/php/php.ini

RUN curl -sS https://getcomposer.org/installer | php -- --install-dir=/usr/local/bin/ --filename=composer
