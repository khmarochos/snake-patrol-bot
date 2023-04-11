from pytz import timezone
from datetime import datetime, timedelta, time
from google.oauth2 import service_account
from googleapiclient.discovery import build
import json
import asyncio
import telegram
import jinja2
import requests

TIMEZONE                    = timezone('Europe/Kyiv')
DAYS_OFFSET                 = 1
GOOGLE_CREDENTIALS_FILE     = './google_credentials.json'
GOOGLE_SPREADSHEET_FILE     = './google_spreadsheet.json'
TELEGRAM_CONFIGURATION_FILE = './telegram_config.json'
NOTIFICATION_TEMPLATE_FILE  = './notification.j2'
STORMGLASS_CREDENTIALS_FILE = './stormglass_credentials.json'


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
        self.spreadsheet_id = spreadsheet_configuration['spreadsheet_id']
        self.range = spreadsheet_configuration['range']
        self.credentials_file = credentials_file
        self.credentials = service_account.Credentials.from_service_account_file(self.credentials_file)
        self.spreadsheet = build('sheets', 'v4', credentials=self.credentials).spreadsheets()
        self.spreadsheet_data = self.spreadsheet.values().get(spreadsheetId=self.spreadsheet_id,
                                                              range=self.range).execute()
        self.spreadsheet_values = self.spreadsheet_data.get('values', [])

    def find_shifts(self, tz: datetime.tzinfo, days: int):
        now = datetime.now(tz)
        timestamp_range_start = tz.localize(datetime.combine(now.date() + timedelta(days=days), time.min)).timestamp()
        timestamp_range_end = tz.localize(datetime.combine(now.date() + timedelta(days=days + 1), time.min)).timestamp()
        tomorrow_shifts = []
        for row in self.spreadsheet_values:
            if row[0].isdigit() and timestamp_range_start <= float(row[0]) < timestamp_range_end:
                tomorrow_shifts.append(row)
        return tomorrow_shifts

    def assign_people(self, shift):
        people = []
        for i in range(2, len(shift) - 1):
            if shift[i] == '0':
                people.append(i)
        return (people)

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


def timestamp2date(timestamp):
    return TIMEZONE.localize(datetime.fromtimestamp(int(timestamp))).strftime("%d.%m.%Y")


def timestamp2time(timestamp):
    return TIMEZONE.localize(datetime.fromtimestamp(int(timestamp))).strftime("%H:%M")


def main():
    # Form the schedule for the tomorrow day
    planner = Planner(spreadsheet_file=GOOGLE_SPREADSHEET_FILE, credentials_file=GOOGLE_CREDENTIALS_FILE)
    schedule = planner.form_schedule(tz=TIMEZONE, days=DAYS_OFFSET)
    # Form the notification text
    time_start = min(list(map(lambda shift: shift['time_start'], schedule)))
    time_end = max(list(map(lambda shift: shift['time_end'], schedule)))
    secondary_group = int(time_start) / 86400 % 2
    weather = []
    with open(STORMGLASS_CREDENTIALS_FILE, 'r') as stormglass_credentials:
        stormglass_credentials = json.load(stormglass_credentials)
    weather_response = requests.get(
        'https://api.stormglass.io/v2/weather/point',
        params={
            'lat': '50.435664',
            'lng': '30.618628',
            'params': 'airTemperature,pressure,cloudCover,gust,humidity,precipitation,visibility',
            'start': str(time_start),
            'end': str((time_start + 86400)),
            'source': 'sg'
        },
        headers={
            'Authorization': stormglass_credentials['stormglass_api_key']
        }
    )
    if weather_response.status_code == 200:
        weather_response = json.loads(weather_response.content)
        for hour_offset, forecast_piece in enumerate(weather_response['hours']):
            forecast_piece_time = time_start + hour_offset * 3600
            if time_start <= forecast_piece_time < time_end:
                forecast_piece['time_start'] = forecast_piece_time
                forecast_piece['time_end'] = forecast_piece_time + 3600
                weather.append(forecast_piece)
    renderer = jinja2.Environment(loader=jinja2.FileSystemLoader('.'))
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
    notifier = Notifier(configuration_file=TELEGRAM_CONFIGURATION_FILE)
    asyncio.run(notifier.notify_people(notification))


if __name__ == '__main__':
    main()
