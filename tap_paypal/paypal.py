"""PayPal API Client."""  # noqa: WPS226
# -*- coding: utf-8 -*-

import logging
import time
from datetime import datetime, timedelta, timezone
from types import MappingProxyType
from typing import Generator, Optional

import httpx
import singer
from dateutil.parser import isoparse
from dateutil.rrule import DAILY, rrule

from tap_paypal.cleaners import clean_paypal_transactions

# Todo: Sadbox
API_SCHEME: str = 'https://'
API_BASE_URL: str = 'api-m.paypal.com'
API_BASE_URL_SANDBOX: str = 'api-m.sandbox.paypal.com'
API_VERSION: str = 'v1'

API_PATH_OAUTH: str = 'oauth2/token'
API_PATH_TRANSACTIONS: str = 'reporting/transactions'

HEADERS: MappingProxyType = MappingProxyType({  # Frozen dictionary
    'Content-Type': 'application/json',
    'Authorization': 'Bearer :accesstoken:',
})

HTTP_TIMEOUT: int = 30
MAX_RETRIES: int = 8
RETRY_BACKOFF_BASE: int = 2
REQUEST_DELAY: float = 1.0
BATCH_DELAY: float = 5.0
RATE_LIMIT_BACKOFF: int = 60
MAX_RESULTSET: int = 10000
MIN_BATCH_HOURS: int = 1


class PayPal(object):  # noqa: WPS230
    """PayPal API Client."""

    def __init__(
        self,
        client_id: str,
        secret: str,
        sandbox=False,
    ) -> None:
        """Initialize client.

        Arguments:
            client_id {str} -- PayPal client id
            secret {str} -- PayPal secret
            sandbox {bool} -- Whether to use the sandbox or live environment
        """
        self.client_id: str = client_id
        self.secret: str = secret
        self.sandbox: bool = sandbox
        self.base: str = API_BASE_URL_SANDBOX if sandbox else API_BASE_URL
        self.logger: logging.Logger = singer.get_logger()

        self.token: Optional[str] = None
        self.token_expires: Optional[datetime] = None
        self.client: httpx.Client = httpx.Client(
            http2=True,
            timeout=HTTP_TIMEOUT,
        )

        if sandbox:
            self.logger.info('Running in Sandbox environment')
        else:
            self.logger.info('Running in Live environment')

        # Perform authentication during initialising
        self._authenticate()

    def paypal_transactions(  # noqa: WPS210, WPS213
        self,
        **kwargs: dict,
    ) -> Generator[dict, None, None]:
        """Paypal transaction history.

        Raises:
            ValueError: When the parameter start_date is missing

        Yields:
            Generator[dict] -- Yields PayPal transactions
        """
        self.logger.info('Stream PayPal transactions')

        # Validate the start_date value exists
        start_date_input: str = str(kwargs.get('start_date', ''))

        if not start_date_input:
            raise ValueError('The parameter start_date is required.')

        # Set start date and end date
        start_date: datetime = isoparse(start_date_input)
        end_date: datetime = datetime.now(timezone.utc).replace(microsecond=0)

        self.logger.info(
            f'Retrieving transactions from {start_date} to {end_date}',
        )
        # Extra kwargs will be converted to parameters in the API requests
        # start_date is parsed into batches, thus we remove it from the kwargs
        kwargs.pop('start_date', None)

        # The difference between start_date and end_date can max be 31 days
        # Split up the requests into weekly batches
        batches: rrule = rrule(
            DAILY,
            dtstart=start_date,
            until=end_date,
        )

        total_batches: int = len(list(batches))
        self.logger.info(f'Total daily batches: {total_batches}')

        current_batch: int = 0

        for start_date_batch in batches:
            end_date_batch: datetime = (
                start_date_batch + timedelta(days=1, seconds=-1)
            )

            # Prevent the end_date from going into the future
            if end_date_batch > end_date:
                end_date_batch = end_date

            # Convert the datetimes to datetime formats the api expects
            start_date_str: str = self._date_to_paypal_format(start_date_batch)
            end_date_str: str = self._date_to_paypal_format(end_date_batch)

            current_batch += 1

            if current_batch > 1:
                time.sleep(BATCH_DELAY)

            self.logger.info(
                f'Parsing batch {current_batch}: {start_date_str} <--> '
                f'{end_date_str}',
            )

            yield from self._fetch_batch_pages(
                start_date_batch,
                end_date_batch,
                current_batch,
                total_batches,
                kwargs,
            )

        self.logger.info('Finished: paypal_transactions')

    def _fetch_batch_pages(  # noqa: WPS210, WPS213, WPS231
        self,
        batch_start: datetime,
        batch_end: datetime,
        current_batch: int,
        total_batches: int,
        extra_params: dict,
    ) -> Generator[dict, None, None]:
        """Fetch all pages for a time window, splitting if too large.

        Arguments:
            batch_start {datetime} -- Start of batch window
            batch_end {datetime} -- End of batch window
            current_batch {int} -- Current batch number for logging
            total_batches {int} -- Total batches for logging
            extra_params {dict} -- Additional query parameters

        Yields:
            Generator[dict] -- Yields cleaned transactions
        """
        start_str = self._date_to_paypal_format(batch_start)
        end_str = self._date_to_paypal_format(batch_end)

        fixed_params: dict = {
            'fields': 'all',
            'page_size': 100,
            'page': 1,
            'start_date': start_str,
            'end_date': end_str,
        }
        http_params: dict = {**fixed_params, **extra_params}

        page: int = 0
        total_pages: int = 1
        url: str = (
            f'{API_SCHEME}{self.base}/'
            f'{API_VERSION}/{API_PATH_TRANSACTIONS}'
        )

        while page < total_pages:
            page += 1
            http_params['page'] = page

            if page > 1:
                time.sleep(REQUEST_DELAY)

            response = self._request_with_retry(url, http_params)
            response_data: dict = response.json()

            if self._is_resultset_too_large(response_data):
                yield from self._split_and_fetch(
                    batch_start,
                    batch_end,
                    current_batch,
                    total_batches,
                    extra_params,
                )
                return

            page = response_data.get('page', 1)
            total_pages = response_data.get('total_pages', 0)

            percentage_page: float = round((page / total_pages) * 100, 2)
            percentage_batch: float = round(
                (current_batch / total_batches) * 100, 2,
            )

            self.logger.info(
                f'Batch: {current_batch} of '  # noqa: WPS221
                f'{total_batches} '
                f'({percentage_batch}%), '
                f'page: {page} of '
                f'{total_pages} '
                f'({percentage_page}%)',
            )

            transactions: list = response_data.get(
                'transaction_details',
                [],
            )
            yield from (
                clean_paypal_transactions(transaction)
                for transaction in transactions
            )

    def _is_resultset_too_large(self, response_data: dict) -> bool:
        """Check if a response indicates the result set is too large."""
        return response_data.get('name') == 'RESULTSET_TOO_LARGE'

    def _split_and_fetch(  # noqa: WPS210
        self,
        batch_start: datetime,
        batch_end: datetime,
        current_batch: int,
        total_batches: int,
        extra_params: dict,
    ) -> Generator[dict, None, None]:
        """Split a time window in half and fetch each sub-batch.

        Arguments:
            batch_start {datetime} -- Start of the window
            batch_end {datetime} -- End of the window
            current_batch {int} -- Current batch number for logging
            total_batches {int} -- Total batches for logging
            extra_params {dict} -- Additional query parameters

        Yields:
            Generator[dict] -- Yields cleaned transactions
        """
        duration = batch_end - batch_start
        hours = duration.total_seconds() / 3600

        if hours < MIN_BATCH_HOURS:
            self.logger.error(
                f'Cannot split further: window is {hours:.1f}h '
                f'({batch_start} to {batch_end}). Skipping.',
            )
            return

        midpoint = (batch_start + (duration / 2)).replace(microsecond=0)

        self.logger.info(
            f'RESULTSET_TOO_LARGE: splitting {hours:.0f}h window into '
            f'two {hours / 2:.0f}h sub-batches',
        )

        time.sleep(BATCH_DELAY)
        yield from self._fetch_batch_pages(
            batch_start,
            midpoint,
            current_batch,
            total_batches,
            extra_params,
        )

        time.sleep(BATCH_DELAY)
        yield from self._fetch_batch_pages(
            midpoint + timedelta(seconds=1),
            batch_end,
            current_batch,
            total_batches,
            extra_params,
        )

    def _authenticate(self) -> None:  # noqa: WPS210
        """Generate a bearer access token."""
        url: str = (
            f'{API_SCHEME}{self.base}/'
            f'{API_VERSION}/{API_PATH_OAUTH}'
        )
        headers: dict = {
            'Accept': 'application/json',
            'Accept-Language': 'en_US',
        }
        post_data: dict = {'grant_type': 'client_credentials'}

        now: datetime = datetime.utcnow()

        response: httpx._models.Response = self.client.post(  # noqa: WPS437
            url,
            headers=headers,
            data=post_data,
            auth=(self.client_id, self.secret),
        )

        # Raise error on 4xx and 5xxx
        response.raise_for_status()

        response_data: dict = response.json()

        # Save the token
        self.token = response_data.get('access_token')

        # Set experation date for token
        expires_in: int = response_data.get('expires_in', 0)
        self.token_expires = now + timedelta(expires_in)

        # Set up headers to use in API requests
        self._create_headers()

        appid: Optional[str] = response_data.get('app_id')

        self.logger.info(
            'Authentication succesfull '
            f'(appid: {appid})',
        )

    def _request_with_retry(  # noqa: WPS210
        self,
        url: str,
        params: dict,
    ) -> httpx._models.Response:  # noqa: WPS437
        """Make a GET request with retry and exponential backoff.

        Arguments:
            url {str} -- Request URL
            params {dict} -- Query parameters

        Raises:
            last_exception: Re-raises the last exception after all retries

        Returns:
            httpx._models.Response -- API response
        """
        last_exception = None

        for attempt in range(1, MAX_RETRIES + 1):
            try:
                response = self.client.get(
                    url,
                    headers=self.headers,
                    params=params,
                )

                if response.status_code == 400:
                    response_body = response.json()
                    if response_body.get('name') == 'RESULTSET_TOO_LARGE':
                        return response

                if response.status_code == 429:
                    retry_after = int(
                        response.headers.get(
                            'Retry-After',
                            RATE_LIMIT_BACKOFF,
                        ),
                    )
                    self.logger.warning(
                        f'Rate limited (429) on attempt '
                        f'{attempt}/{MAX_RETRIES}. '
                        f'Waiting {retry_after}s...',
                    )
                    raise httpx.HTTPStatusError(
                        f'Rate limited (429)',
                        request=response.request,
                        response=response,
                    )

                if response.status_code >= 500:
                    self.logger.warning(
                        f'PayPal API server error {response.status_code} '
                        f'(attempt {attempt}/{MAX_RETRIES})',
                    )
                    raise httpx.HTTPStatusError(
                        f'Server error {response.status_code}',
                        request=response.request,
                        response=response,
                    )

                if response.status_code >= 400:
                    self.logger.error(
                        f'PayPal API error {response.status_code}: '
                        f'{response.text}',
                    )
                response.raise_for_status()
                return response

            except (httpx.ReadTimeout, httpx.ConnectTimeout,
                    httpx.HTTPStatusError) as exc:
                last_exception = exc
                if attempt < MAX_RETRIES:
                    is_rate_limited = (
                        isinstance(exc, httpx.HTTPStatusError)
                        and exc.response.status_code == 429
                    )
                    if is_rate_limited:
                        wait_time = int(
                            exc.response.headers.get(
                                'Retry-After',
                                RATE_LIMIT_BACKOFF,
                            ),
                        )
                    else:
                        wait_time = RETRY_BACKOFF_BASE ** attempt
                    self.logger.warning(
                        f'Request failed (attempt {attempt}/{MAX_RETRIES}): '
                        f'{exc}. Retrying in {wait_time}s...',
                    )
                    time.sleep(wait_time)
                else:
                    self.logger.critical(str(exc))

        raise last_exception

    def _create_headers(self) -> None:
        """Create authenticationn headers for requests."""
        headers: dict = dict(HEADERS)
        headers['Authorization'] = headers['Authorization'].replace(
            ':accesstoken:',
            self.token,
        )
        self.headers = headers

    def _date_to_paypal_format(self, input_datetime: datetime) -> str:
        """Convert a datetime to the format that the PayPal api expects.

        Arguments:
            input_datetime {datetime} -- Input e.g. 2021-01-01 00:00:00+00:00

        Returns:
            str -- Converted datetime: 2021-01-01T00:00:00+0000
        """
        clean_dt = input_datetime.replace(microsecond=0)
        return ''.join(clean_dt.isoformat().rsplit(':', 1))
