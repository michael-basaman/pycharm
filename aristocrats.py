import requests
import time
import psycopg2
from datetime import datetime, date, timedelta
import pytz
import logging


logger = logging.getLogger(__name__)
logger.setLevel(logging.INFO)
file_handler = logging.FileHandler("/var/log/python/aristocrats.log")
#file_handler = logging.FileHandler("C:/Users/state/logs/aristocrats.log")
file_handler.setLevel(logging.INFO)
formatter_file = logging.Formatter('%(asctime)s %(levelname)s - %(message)s')
file_handler.setFormatter(formatter_file)
logger.addHandler(file_handler)


def load_aristocrats():
    conn = psycopg2.connect(database="trade", user="trade", password="trade", host="localhost", port=5432)

    aristocrats = ["AOS","ABT","ABBV","AFL","APD","ALB","AMCR","ADM","ATO","ADP","BDX","BRO",
            "BF.B","CAH","CAT","CHRW","CVX","CB","CHD","CINF","CTAS","CLX","KO","CL","ED",
            "DOV","ECL","EMR","ERIE","ES","ESS","EXPD","XOM","FDS","FAST","FRT","BEN",
            "GD","GPC","HRL","ITW","IBM","SJM","JNJ","KVUE","KMB","LIN","LOW","MKC",
            "MCD","MDT","NEE","NDSN","NUE","PNR","PEP","PPG","PG","O","ROP","SPGI",
            "SHW","SWK","SYY","TROW","TGT","GWW","WMT","WST"]

    aristocrats = sorted(aristocrats)

    cursor5 = conn.cursor()
    for aristocrat in aristocrats:
        cursor5.execute("""
        INSERT INTO aristocrats (ticker)
        VALUES (%s)
        """, (aristocrat,))
    conn.commit()


def convert_utc_to_timezone(utc_datetime, timezone_str):
    try:
        utc_timezone = pytz.utc
        local_timezone = pytz.timezone(timezone_str)

        utc_datetime = utc_datetime.replace(tzinfo=utc_timezone)
        local_datetime = utc_datetime.astimezone(local_timezone)

        return local_datetime
    except ValueError:
        return None


def run():
    conn = psycopg2.connect(database="trade", user="trade", password="trade", host="localhost", port=5432)
    api_key = "pf1MEQavmoWso5oE2fMxvlywqA7ALIlU"
    urls = []

    def submit_requests():
        last_time = time.time()

        while len(urls) > 0:
            now = time.time()
            elapsed = now - last_time

            if elapsed < 15:
                time.sleep(1)
                continue

            type, url = urls.pop(0)

            r = requests.get(url)
            data = r.json()
            last_time = time.time()

            if type == "DIVIDEND":
                if "status" in data and data["status"] == "OK":
                    if "results" in data:
                        results = data["results"]

                        for result in results:
                            if "pay_date" in result \
                                    and "cash_amount" in result \
                                    and "ticker" in result:
                                pay_date = result["pay_date"]
                                cash_amount = result["cash_amount"]
                                ticker = result["ticker"]

                                cursor = conn.cursor()

                                cursor.execute("""
                                SELECT cash_amount
                                FROM dividends
                                WHERE ticker = %s
                                AND pay_date = %s
                                """, (ticker, pay_date,))

                                record = cursor.fetchone()

                                if record is None:
                                    cursor.execute("""
                                    INSERT INTO dividends (ticker, pay_date, cash_amount)
                                    VALUES (%s, %s, %s)
                                    """, (ticker, pay_date, cash_amount,))
                                    conn.commit()
                                    logger.info(f"Inserted Dividend ({ticker}, {pay_date}, {cash_amount})")
                                elif (float(record[0]) - cash_amount) >= 0.000001:
                                    cursor.execute("""
                                    UPDATE dividends
                                    SET cash_amount = %s
                                    WHERE ticker = %s
                                    AND pay_date = %s
                                    """, (cash_amount, ticker, pay_date,))
                                    conn.commit()
                                    logger.info(f"Updated Dividend ({ticker}, {pay_date}, {cash_amount})")
                                else:
                                    logger.info(f"Unchanged Dividend ({ticker}, {pay_date}, {cash_amount})")
            elif type == "CLOSE":
                if "status" in data and data["status"] == "OK":
                    if "from" in data \
                            and "symbol" in data \
                            and "open" in data \
                            and "high" in data \
                            and "low" in data \
                            and "close" in data:
                        stock_from = data["from"]
                        stock_symbol = data["symbol"]
                        stock_open = data["open"]
                        stock_high = data["high"]
                        stock_low = data["low"]
                        stock_close = data["close"]

                        cursor = conn.cursor()

                        cursor.execute("""
                        SELECT price_close
                        FROM equities
                        WHERE ticker = %s
                        AND price_date = %s
                        """, (stock_symbol, stock_from,))

                        record = cursor.fetchone()

                        if record is None:
                            cursor.execute("""
                            INSERT INTO equities (ticker, price_date, price_open, price_high, price_low, price_close)
                            VALUES (%s, %s, %s, %s, %s, %s)
                            """, (stock_symbol, stock_from, stock_open, stock_high, stock_low, stock_close,))
                            conn.commit()
                            logger.info(f"Inserted Equity ({stock_symbol}, {stock_from}, {stock_close})")
                        elif (float(record[0]) - stock_close) >= 0.000001:
                            logger.info(f"Price mismatch - expected: {record[0]}, actual: {stock_close}")

                            cursor.execute("""
                            UPDATE equities
                            SET price_open = %s,
                            price_high = %s,
                            price_low = %s,
                            price_close = %s
                            WHERE ticker = %s
                            AND price_date = %s
                            """, (stock_open, stock_high, stock_low, stock_close, stock_symbol, stock_from,))
                            logger.info(f"Updated Equity ({stock_symbol}, {stock_from}, {stock_close})")
                        else:
                            logger.info(f"Unchanged Equity ({stock_symbol}, {stock_from}, {stock_close})")

    today = datetime.now()
    timezone_str = "America/New_York"
    local_datetime = convert_utc_to_timezone(today, timezone_str)

    weekday = local_datetime.weekday()
    logger.info(f"hour: {local_datetime.hour}")

    if weekday == 6:
        local_datetime = local_datetime - timedelta(days=2)
    elif weekday == 5:
        local_datetime = local_datetime - timedelta(days=1)
    elif local_datetime.hour < 20:
        if weekday == 0:
            local_datetime = local_datetime - timedelta(days=3)
        else:
            local_datetime = local_datetime - timedelta(days=1)

    local_date = local_datetime.strftime("%Y-%m-%d")
    logger.info(f"date: {local_date}")

    cursor2 = conn.cursor()
    cursor2.execute("""
    SELECT ticker
    FROM aristocrats
    WHERE active IS TRUE
    """)

    cursor3 = conn.cursor()
    cursor3.execute("""
    SELECT ticker
    FROM equities
    WHERE price_date = %s
    """, (local_date,))

    equities_records = cursor3.fetchall()

    loaded = set()

    for equities_record in equities_records:
        loaded.add(equities_record[0])

    ticker_records = cursor2.fetchall()

    symbols = []

    for ticker_record in ticker_records:
        if ticker_record[0] not in loaded:
            symbols.append(ticker_record[0])

    for symbol in symbols:
        urls.append(("CLOSE", f"https://api.polygon.io/v1/open-close/{symbol}/{local_date}?adjusted=false&apiKey={api_key}"))
        urls.append(("DIVIDEND", f"https://api.polygon.io/v3/reference/dividends?ticker={symbol}&limit=5&apiKey={api_key}"))

    logger.info(f"{len(urls)} requests")

    if len(urls):
        submit_requests()
        logger.info("done")


last_time = 0

while True:
    try:
        now = time.time()
        elapsed = now - last_time

        if elapsed > 900:
            logger.info("running")
            run()
            last_time = time.time()
    except Exception as e:
        logger.error(f"{str(e)}")
    finally:
        time.sleep(300)
