import time
import pytz
import datetime
import json
import databox
import asyncio
import aiohttp

from dateutil.relativedelta import relativedelta

TIME_BEGIN = datetime.datetime.now()


#######################################################################################################################
#
# Class definition
#
#######################################################################################################################

class StackAdapt:

    """ Stack Adapt class

    Attributes:
        <class> loop: Asyncio event loop
        <int> requests: Total number of API request made
        <dict> requestHeaders: HTTP Request Headers
        <class> requestTimeout: Asyncio ClientTimeout instance
        <str> requestURL: API Base URL
        <class> semaphore: Asyncio semaphore
        <str> token: Data Source token
    """

    def __init__(self, _semaphore_size_=5):

        """Initializes the instance

        Args:
            <int> _semaphore_size_: Asyncio semaphore size
        """

        self.DataboxToken = '9d13e8b8b35740fa8abeaec44236793d'
        self.requests = 0
        self.requestHeaders = {
            'Accept': 'application/json',
            'Content-Type': 'application/json',
            'Authorization': 'Bearer c9968b4246953bb4170d120e2c08320b39fb1774604d8ca9688fedad44d3e082'
        }
        self.requestTimeout = aiohttp.ClientTimeout(total=None, sock_connect=240, sock_read=240)
        self.requestURL = 'https://api.stackadapt.com/graphql'
        self.semaphore = asyncio.Semaphore(_semaphore_size_)

        self.loop = asyncio.get_event_loop()
        self.loop.set_exception_handler(self.exception)

    def exception(self, _loop_, _context_):

        """Handles exceptions raised during pushes

        Args:
            <class> _loop_: Asyncio event loop
            <exception> _context_: Exception raised

        Returns:
            <void>
        """

        self.loop.default_exception_handler(_context_)

        exception = _context_.get('exception')
        if isinstance(exception, Exception):
            print(_context_)
            self.loop.stop()

    async def fetch(self, query_str):

        """Makes an API request to Service Titan API

        Args:
            <str> _token_: Data Source token
            <dict> _body_: API request body

        Returns:
            <dict>: Dictionary with API response data
        """

        client = aiohttp.ClientSession(headers=self.requestHeaders, timeout=self.requestTimeout)

        async with self.semaphore:
            async with client as session:
                url = self.requestURL

                print('Stack Adapt >', url)
                body = {"query": query_str}

                async with session.post(url=url, json=body) as response:
                    result = {
                        'body': body,
                        'code': response.status,
                        'reason': response.reason,
                        'data': json.loads(await response.text())
                    }
                    self.requests += 1
                    return result

    @staticmethod
    def period_start(_offset_):

        """Calculates the start date for sync period

        Args:
            <int> _offset_: Last X months

        Returns:
            <list>: List of API compatible Date strings
        """

        tz = pytz.timezone('UTC')
        dt_today = datetime.datetime.fromtimestamp(time.time(), tz=tz)
        dt_start = (dt_today - relativedelta(days=_offset_))
        dates = []

        while dt_start < dt_today:
            dates.append(dt_start.strftime('%Y-%m-%d'))
            dt_start = dt_start + datetime.timedelta(days=1)

        return dates

    async def campaigns(self):

        """ Fetches all campaigns

        Args:
            <void>

        Returns:
            <list>: A key value pair dict, where keys are campaign ids and values are campaign names
        """

        data = {}
        after = ''
        while True:
            query_str = F'query {{\n  campaigns(\n    after: "{after}"\n  ) {{\n    nodes {{\n      name\n' \
                        F'id\n    ' \
                        F'}}\n    pageInfo {{\n      endCursor\n      hasNextPage\n    }}\n  }}\n}}'
            result = await self.fetch(query_str)
            page_info = result['data']['data']['campaigns']['pageInfo']
            has_next_page = page_info['hasNextPage']
            nodes = result['data']['data']['campaigns']['nodes']
            for node in nodes:
                data[node['id']] = node['name']

            if has_next_page:
                after = page_info['endCursor']
            else:
                break

        return data

    async def footfall_report(self, campaigns_dict, start_date):

        """Fetches all relevant reports
        Args:
            <list> campaigns_dict: List of campaigns
            <int> start_date: Date to sync data for

        Returns:
            <void>
        """

        data = []

        for campaign_id, campaign_name in campaigns_dict.items():
            start_date_time = F'{start_date}T12:00:00.000Z'
            query_str = F'query {{\n  footfallTrackingStats(filterBy: {{\n    locationSetIds: [882],' \
                        F'\n     campaignIds: [\"{campaign_id}\"]' \
                        F'\n     startTime: \"{start_date_time}\",' \
                        F'\n    endTime: \"{start_date_time}\"\n  }}) {{' \
                        F'\n    graphData {{\n      ' \
                        F'localDayOfWeek {{\n        timeString\n        footfallConv\n      }}\n      ' \
                        F'localHourOfDay ' \
                        F'{{\n        timeString\n        footfallConv\n     }}\n    }}\n    summaryData{{\n      ' \
                        F'footfallConv\n      totalCost\n      averageEcpv\n      visitationLift\n      pValue\n  ' \
                        F'    ' \
                        F'startDate\n      endDate\n    }}\n  }}\n}}'
            # Gets all the data
            print('Fetching data >', 'date >', start_date,
                  ' campaign_id >', campaign_id, ' campaign_name >', campaign_name)
            result = await self.fetch(query_str)
            if not result['data']['data']:
                continue

            reports_data = result['data']['data']['footfallTrackingStats']['graphData']
            local_day_of_week_data = reports_data['localDayOfWeek']
            local_hour_of_day_data = reports_data['localHourOfDay']
            summary_data = result['data']['data']['footfallTrackingStats']['summaryData']

            databox.append(self.DataboxToken, {
                '$Visits': summary_data['footfallConv'],
                'date': start_date_time,
                'Campaign': campaign_name
            })

            for report in local_day_of_week_data:
                data.append({
                    '$Visits': report['footfallConv'],
                    'date': start_date_time,
                    report["timeString"]: campaign_name
                })
                databox.append(self.DataboxToken, {
                    '$Visits': report['footfallConv'],
                    'date': start_date_time,
                    report["timeString"]: campaign_name
                })

            for report in local_hour_of_day_data:
                data.append({
                    '$Visits': report['footfallConv'],
                    'date': start_date_time,
                    report["timeString"]: campaign_name
                })
                databox.append(self.DataboxToken, {
                    '$Visits': report['footfallConv'],
                    'date': start_date_time,
                    report["timeString"]: campaign_name
                })

        print('Data for ', start_date, ' = ', data)

    async def sync(self, _offset_=0):

        campaigns_dict = await self.campaigns()
        dates = self.period_start(_offset_)

        for date in dates:
            asyncio.create_task(self.footfall_report(campaigns_dict, date))


#######################################################################################################################
#
# Define Metrics
#
#######################################################################################################################
async def main(_days_offset_):

    stack_adapt = StackAdapt()

    # await stack_adapt.footfall_report(_offset_=_days_offset_)
    await stack_adapt.sync(_offset_=_days_offset_)

    await asyncio.gather(*asyncio.all_tasks() - {asyncio.current_task()})


#######################################################################################################################
#
# Execute
#
#######################################################################################################################

databox = databox.Databox()

asyncio.set_event_loop_policy(asyncio.WindowsSelectorEventLoopPolicy())

asyncio.run(main(_days_offset_=5))

databox.push()

print(datetime.datetime.now() - TIME_BEGIN)
