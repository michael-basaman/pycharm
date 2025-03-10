import json
import psycopg2
import requests
import time


def get_access_token():
    conn = psycopg2.connect(database="tiktok", user="tiktok", password="tiktok", host="localhost", port=5432)

    cursor = conn.cursor()

    cursor.execute("""
    SELECT state, code
    FROM oauth
    WHERE create_datetime > now() - INTERVAL '1 hour'
    ORDER BY create_datetime DESC
    LIMIT 1
    """)

    record = cursor.fetchone()

    if record is None:
        with open('/var/log/oauth_tiktok.log', 'a') as o:
            o.write("ERROR: get_access_token() - No active oauth records\n")
        return None, None, None

    state = record[0]
    code = record[1]

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cache-Control": "no-cache",
    }

    data = {
        "client_key": "x",
        "client_secret": "x",
        "code": code,
        "grant_type": "authorization_code",
        "redirect_uri": "https://www.anticirculatory.com/tiktok/redirect",
    }

    response = requests.post("https://open.tiktokapis.com/v2/oauth/token/", headers=headers, data=data)

    if response.status_code not in {200, 201}:
        with open('/var/log/oauth_tiktok.log', 'a') as o:
            o.write(f'ERROR: get_access_token() - status_code: {response.status_code}\n')
        return None, None, None

    text = response.text
    parsed = json.loads(text)

    if "error" in parsed:
        with open('/var/log/oauth_tiktok.log', 'a') as o:
            o.write(f'ERROR: get_access_token() - {parsed["error_description"]}\n')
        return None, None, None

    access_tokens = parsed["access_token"]
    expires_in = int(parsed["expires_in"])
    refresh_token = parsed["refresh_token"]
    refresh_expires_in = parsed["refresh_expires_in"]
    open_id = parsed["open_id"]

    cursor = conn.cursor()

    cursor.execute(
        """
        UPDATE oauth
        SET access_token = %s, refresh_token = %s, open_id = %s, update_datetime = now(), expiry_datetime = now() + INTERVAL '%s seconds'
        WHERE state = %s
        """,
        (access_tokens, refresh_token, open_id, expires_in, state))
    conn.commit()

    with open('/var/log/oauth_tiktok.log', 'a') as o:
        o.write(f"initial, open_id: {open_id}, access_token: {access_tokens}, expires_in: {expires_in}, refresh_token: {refresh_token}, refresh_expires_in: {refresh_expires_in}\n")

    return state, refresh_token, expires_in


def get_refresh_token(state, refresh_token):
    conn = psycopg2.connect(database="tiktok", user="tiktok", password="tiktok", host="localhost", port=5432)

    cursor = conn.cursor()

    headers = {
        "Content-Type": "application/x-www-form-urlencoded",
        "Cache-Control": "no-cache",
    }

    data = {
        "client_key": "x",
        "client_secret": "x",
        "grant_type": "refresh_token",
        "refresh_token": refresh_token,
    }

    response = requests.post("https://open.tiktokapis.com/v2/oauth/token/", headers=headers, data=data)

    if response.status_code not in {200, 201}:
        with open('/var/log/oauth_tiktok.log', 'a') as o:
            o.write(f'ERROR: get_refresh_token() - status_code: {response.status_code}\n')
        return None, None

    text = response.text
    parsed = json.loads(text)

    if "error" in parsed:
        with open('/var/log/oauth_tiktok.log', 'a') as o:
            o.write(f'ERROR: get_refresh_token() - {parsed["error_description"]}\n')
        return None, None

    access_tokens = parsed["access_token"]
    expires_in = int(parsed["expires_in"])
    refresh_token = parsed["refresh_token"]
    refresh_expires_in = parsed["refresh_expires_in"]
    open_id = parsed["open_id"]

    cursor.execute(
        """
        UPDATE oauth
        SET access_token = %s, refresh_token = %s, open_id = %s, update_datetime = now(), expiry_datetime = now() + INTERVAL '%s seconds'
        WHERE state = %s
        """,
        (access_tokens, refresh_token, open_id, expires_in, state))
    conn.commit()

    with open('/var/log/oauth_tiktok.log', 'a') as o:
        o.write(f"refresh, open_id: {open_id}, access_token: {access_tokens}, expires_in: {expires_in}, refresh_token: {refresh_token}, refresh_expires_in: {refresh_expires_in}\n")

    return refresh_token, expires_in


def run():
    state, refresh_token, expires_in = get_access_token()

    if state is None:
        return

    while True:
        start_time = time.time()

        sleep_for = max(900, expires_in + 60)

        with open('/var/log/oauth_tiktok.log', 'a') as o:
            o.write(f"run() sleeping for {int(sleep_for / 60)} minutes\n")

        while True:
            now = time.time()

            if (now - start_time) > sleep_for:
                refresh_token, expires_in = get_refresh_token(state, refresh_token)

                if refresh_token is None:
                    return

                break
            else:
                time.sleep(30)


def main():
    while True:
        try:
            run()
        except Exception as e:
            with open('/var/log/oauth_tiktok.log', 'a') as o:
                o.write(f"ERROR: {str(e)}\n")
        finally:
            sleep_for = 900
            with open('/var/log/oauth_tiktok.log', 'a') as o:
                o.write(f"main() sleeping for {int(sleep_for / 60)} minutes\n")
            time.sleep(sleep_for)


if __name__ == "__main__":
    main()