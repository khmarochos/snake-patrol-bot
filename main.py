import os
import argparse
import logging
import requests
import asyncio
import pytz
import datetime
import json
import jinja2
import telegram
import google.oauth2
import googleapiclient.discovery


TIMEZONE = pytz.timezone('Europe/Kyiv')
DAYS_OFFSET = 1
PROFILES_SUBDIR = 'profiles'
STORMGLASS_CACHE_SUBDIR = 'stormglass_cache'
STORMGLASS_CACHE_FILE = 'stormglass_cache.json'
STORMGLASS_CREDENTIALS_FILE = 'stormglass_credentials.json'
GOOGLE_CREDENTIALS_FILE = 'google_credentials.json'
GOOGLE_SPREADSHEET_FILE = 'google_spreadsheet.json'
TELEGRAM_CONFIGURATION_FILE = 'telegram_config.json'
NOTIFICATION_TEMPLATE_FILE = 'notification.j2'


class Notifier:
    def __init__(self, configuration_file: str):
        with open(configuration_file, 'r') as telegram_configuration:
            self.telegram_configuration = json.load(telegram_configuration)
        self.telegram_bot = telegram.Bot(self.telegram_configuration['telegram_api_token'])

    async def notify_people(self, notification_text: str) -> None:
        async with self.telegram_bot:
            await self.telegram_bot.send_message(
                text=notification_text,
                chat_id=self.telegram_configuration['chat_id']
            )


class Planner:
    def __init__(self, spreadsheet_file: str, credentials_file: str):
        with open(spreadsheet_file, 'r') as spreadsheet_configuration:
            spreadsheet_configuration = json.load(spreadsheet_configuration)
        credentials = google.oauth2.service_account.Credentials.from_service_account_file(credentials_file)
        self.spreadsheet_values = googleapiclient\
            .discovery\
            .build('sheets', 'v4', credentials=credentials)\
            .spreadsheets()\
            .values()\
            .get(spreadsheetId=spreadsheet_configuration['spreadsheet_id'], range=spreadsheet_configuration['range'])\
            .execute()\
            .get('values', [])

    def find_shifts(self, tz: datetime.tzinfo, days: int):
        now = datetime.datetime.now(tz)
        timestamp_range_start = tz.localize(
            datetime.datetime.combine(
                now.date() + datetime.timedelta(days=days),
                datetime.time.min
            )
        ).timestamp()
        timestamp_range_end = tz.localize(
            datetime.datetime.combine(
                now.date() + datetime.timedelta(days=days + 1),
                datetime.time.min
            )
        ).timestamp()
        tomorrow_shifts = []
        for row in self.spreadsheet_values:
            if len(row) > 0:
                if row[0].isdigit() and timestamp_range_start <= float(row[0]) < timestamp_range_end:
                    tomorrow_shifts.append(row)
        return tomorrow_shifts

    def assign_people(self, shift: list[str]):
        people = []
        for i in range(2, len(shift) - 1):
            if shift[i] == '0':
                people.append(i)
        return people

    def find_person(self, column: int):
        return {
            'name': self.spreadsheet_values[0][column],
            'telegram_handler': self.spreadsheet_values[1][column]
        }

    def form_schedule(self, tz: datetime.tzinfo, days: int):
        schedule = []
        for shift in self.find_shifts(tz=tz, days=days):
            people = self.assign_people(shift)
            schedule.append(
                {
                    'time_start': int(shift[0]),
                    'time_end': int(shift[0]) + 3600 * 2,
                    'people': list(
                        map(lambda column: self.find_person(column), people)
                    )
                }
            )
        return schedule


class Synoptic:
    def __init__(self, stormglass_credentials_file: str, stormglass_cache_file: str):
        self.stormglass_data = {}
        update_cache = True
        current_time = datetime.datetime.now()
        if os.path.isfile(stormglass_cache_file):
            modified_time = datetime.datetime.fromtimestamp(os.path.getmtime(stormglass_cache_file))
            if current_time - modified_time < datetime.timedelta(hours=3):
                update_cache = False
        if update_cache:
            with open(stormglass_credentials_file, 'r') as stormglass_credentials:
                stormglass_credentials = json.load(stormglass_credentials)
            weather_response = requests.get(
                'https://api.stormglass.io/v2/weather/point',
                params={
                    'lat': '50.435664',
                    'lng': '30.618628',
                    'params': 'airTemperature,pressure,cloudCover,gust,humidity,precipitation,visibility',
                    'start': datetime.datetime.timestamp(current_time),
                    'end': datetime.datetime.timestamp(current_time + datetime.timedelta(days=1)),
                    'source': 'sg'
                },
                headers={
                    'Authorization': stormglass_credentials['stormglass_api_key']
                }
            )
            if weather_response.status_code == 200:
                with open(stormglass_cache_file, 'w') as stormglass_cache:
                    stormglass_cache.write(weather_response.content.decode('utf-8'))
        with open(stormglass_cache_file, 'r') as stormglass_cache:
            self.stormglass_data = json.load(stormglass_cache)

    def forecast_for_time_range(self, time_start: int, time_end: int):
        forecast_data = []
        if 'hours' in self.stormglass_data:
            for hour_offset, forecast_piece in enumerate(self.stormglass_data['hours']):
                forecast_piece_time = time_start + hour_offset * 3600
                if time_start <= forecast_piece_time < time_end:
                    forecast_piece['time_start'] = forecast_piece_time
                    forecast_piece['time_end'] = forecast_piece_time + 3600
                    forecast_data.append(forecast_piece)
        return forecast_data


def timestamp2date(timestamp):
    return TIMEZONE.localize(datetime.datetime.fromtimestamp(int(timestamp))).strftime("%d.%m.%Y")


def timestamp2time(timestamp):
    return TIMEZONE.localize(datetime.datetime.fromtimestamp(int(timestamp))).strftime("%H:%M")


def main():

    # Determine the directories' names
    arguments = argparse.ArgumentParser(description='snake-patrol-bot')
    arguments.add_argument('-p', '--profile', required=True, help='profile name')
    arguments = arguments.parse_args()

    home_dir = os.path.dirname(os.path.realpath(__file__))

    stormglass_cache_dir = '/'.join([home_dir, STORMGLASS_CACHE_SUBDIR, ''])
    profiles_dir = '/'.join([home_dir, PROFILES_SUBDIR, arguments.profile, ''])

    # Form the schedule for the tomorrow day
    planner = Planner(
        spreadsheet_file=(profiles_dir + GOOGLE_SPREADSHEET_FILE),
        credentials_file=(profiles_dir + GOOGLE_CREDENTIALS_FILE)
    )
    schedule = planner.form_schedule(tz=TIMEZONE, days=DAYS_OFFSET)
    time_start = min(list(map(lambda shift: shift['time_start'], schedule)))
    time_end = max(list(map(lambda shift: shift['time_end'], schedule)))

    # Get the weather forecast
    weather = None
    try:
        synoptic = Synoptic(
            stormglass_credentials_file=(profiles_dir + STORMGLASS_CREDENTIALS_FILE),
            stormglass_cache_file=(stormglass_cache_dir + STORMGLASS_CACHE_FILE)
        )
        weather = synoptic.forecast_for_time_range(
            time_start=time_start,
            time_end=time_end
        )
    except Exception as e:
        logging.warning(f'No weather forecast available ({e}), have to get on without it')

    # Form the notification text
    secondary_group = int(time_start / 86400 % 2)
    renderer = jinja2.Environment(loader=jinja2.FileSystemLoader(profiles_dir))
    renderer.filters['timestamp2date'] = timestamp2date
    renderer.filters['timestamp2time'] = timestamp2time
    renderer = renderer.get_template(NOTIFICATION_TEMPLATE_FILE)
    notification = renderer.render(
        start_date=time_start,
        schedule=schedule,
        secondary_group=secondary_group,
        weather=weather
    )

    # Send the notification
    notifier = Notifier(configuration_file=(profiles_dir + TELEGRAM_CONFIGURATION_FILE))
    asyncio.run(notifier.notify_people(notification))


if __name__ == '__main__':
    main()
