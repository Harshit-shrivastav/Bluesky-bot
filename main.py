import os
import logging
import sqlite3
import random
import time
import requests 
import sys
from datetime import datetime, timedelta
from atproto import Client, exceptions
from tenacity import retry, stop_after_attempt, wait_exponential, wait_fixed
from pytz import timezone
from typing import Optional
from dotenv import load_dotenv
import threading

load_dotenv()

logging.basicConfig(
    filename='bluesky_bot.log',
    filemode='a',
    format='%(asctime)s - %(levelname)s - %(funcName)s - %(lineno)d - %(levelname)s - %(message)s',
    level=logging.INFO
)

DAILY_FOLLOW_LIMIT = 20
FOLLOW_DELAY_MIN = 60 
FOLLOW_DELAY_MAX = 4320
UNFOLLOW_AFTER_DAYS = 5
REQUIRED_TERMS = ['bsky', 'sky']
POST_TIMEZONE = timezone('Asia/Kolkata')

BASE_URL = "https://api.h-s.site"
def get_assistant_response(user_prompt, system_prompt):
    try:
        try:
            token_response = requests.get(f"{BASE_URL}/v1/get-token")
            token_response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error getting token: {e}")
            return None 
        try:
            token = token_response.json()["token"]
        except KeyError:
            print("Error: 'token' key not found in the response.")
            return None 
        except ValueError:
            print("Error: Invalid JSON response from the server.")
            return None 

        payload = {
            "token": token,
            "model": "gpt-4o-mini",
            "message": [
                {"role": "user", "content": system_prompt},
                {"role": "user", "content": user_prompt}
            ],
            "stream": False
        }

        try:
            response = requests.post(f"{BASE_URL}/v1/chat/completions", json=payload)
            response.raise_for_status()
        except requests.exceptions.RequestException as e:
            print(f"Error sending request to chat completions: {e}")
            return None 

        try:
            response_data = response.json()
            content = response_data["choice"][0]["message"]["content"]
            return content
        except KeyError as e:
            print(f"Error: Missing expected key in the response - {e}")
            return None
        except IndexError:
            print("Error: No choices found in the response.")
            return None
        except ValueError:
            print("Error: Invalid JSON response from the server.")
            return None

    except Exception as e:
        print(f"An unexpected error occurred: {e}")
        return None


class BlueskyBot:
    @retry(stop=stop_after_attempt(3), wait=wait_fixed(10))
    def login(self):
        self.client.login(os.getenv('BLUESKY_HANDLE'), os.getenv('BLUESKY_PASSWORD'))
        logging.info("Successfully logged into Bluesky")

    def __init__(self):
        self.client = Client()
        self.connect_db()
        self.login()

    def connect_db(self):
        try:
            self.conn = sqlite3.connect('bluesky_follows.db')
            self.cursor = self.conn.cursor()
            self.cursor.execute('''
                CREATE TABLE IF NOT EXISTS followed_users (
                    did TEXT PRIMARY KEY,
                    handle TEXT,
                    followed_at TIMESTAMP,
                    unfollowed BOOLEAN DEFAULT 0
                )
            ''')
            self.conn.commit()
            logging.info("Database initialized successfully")
        except Exception as e:
            logging.error(f"Database initialization failed: {str(e)}")
            raise

    @retry(stop=stop_after_attempt(3), wait=wait_exponential(multiplier=1))
    def get_suggestions(self):
        try:
            response = self.client.get_actor_suggestions()
            return response.actors
        except exceptions.RateLimitError:
            logging.warning("Rate limit hit on get_suggestions")
            raise
        except Exception as e:
            logging.error(f"Error getting suggestions: {str(e)}", exc_info=True)
            raise

    def check_criteria(self, user):
        try:
            if not any(term in user.handle.lower() for term in REQUIRED_TERMS):
                return False

            self.cursor.execute('SELECT did FROM followed_users WHERE did = ? AND unfollowed = 0', (user.did,))
            return self.cursor.fetchone() is None
        except Exception as e:
            logging.error(f"Error checking criteria: {str(e)}", exc_info=True)
            return False

    @retry(stop=stop_after_attempt(2), wait=wait_exponential(multiplier=1))
    def follow_user(self, user):
        try:
            self.client.follow(user.did)
            self.cursor.execute('INSERT INTO followed_users (did, handle, followed_at) VALUES (?, ?, ?)',
                                (user.did, user.handle, datetime.now()))
            self.conn.commit()
            logging.info(f"Successfully followed {user.handle}")
            return True
        except exceptions.RateLimitError:
            logging.warning(f"Rate limit hit following {user.handle}")
            raise
        except Exception as e:
            logging.error(f"Error following {user.handle}: {str(e)}", exc_info=True)
            return False

    def daily_post(self):
        system_prompt = "You are a creative social media assistant specializing in engaging Bluesky posts."
        user_prompt = "Create a inspirational post about productivity. Include 2 mental health hashtags."
        
        post_text = get_assistant_response(user_prompt, system_prompt)
        
        if post_text:
            try:
                self.post_to_bluesky(post_text)
            except Exception as e:
                logging.error(f"Failed to post: {str(e)}", exc_info=True)
        else:
            logging.warning("AI generation failed, retrying in 1 hour")
            time.sleep(3600)

    def follow_cycle(self):
        follow_count = 0
        start_time = datetime.now()

        while True:
            try:
                self.check_unfollows()

                if follow_count < DAILY_FOLLOW_LIMIT:
                    suggestions = self.get_suggestions()
                    if not suggestions:
                        logging.warning("No follow suggestions available. Sleeping for 1 hour.")
                        time.sleep(3600)
                        continue

                    random.shuffle(suggestions)

                    for user in suggestions:
                        if self.check_criteria(user):
                            if self.follow_user(user):
                                follow_count += 1
                                delay = random.randint(FOLLOW_DELAY_MIN, FOLLOW_DELAY_MAX)
                                logging.info(f"Sleeping for {delay//60} minutes")
                                time.sleep(delay)

                            if follow_count >= DAILY_FOLLOW_LIMIT:
                                break

                    if not suggestions:
                        time.sleep(3600)

                else:
                    remaining = (start_time + timedelta(hours=24) - datetime.now()).total_seconds()
                    logging.info(f"Daily follow limit reached. Sleeping for {remaining:.0f} seconds")
                    time.sleep(remaining)
                    follow_count = 0
                    start_time = datetime.now()
            except Exception as e:
                logging.error(f"Follow cycle error: {str(e)}", exc_info=True)
                time.sleep(300)

    def run(self):
        try:
            while True:
                wait_time = self.schedule_next_post()
                time.sleep(wait_time)
                self.daily_post()
                
                follow_thread = threading.Thread(target=self.follow_cycle, daemon=True)
                follow_thread.start()
                follow_thread.join()

        except KeyboardInterrupt:
            logging.info("Shutting down...")
        finally:
            self.conn.close()

if __name__ == "__main__":
    required_vars = ['BLUESKY_HANDLE', 'BLUESKY_PASSWORD']
    missing = [var for var in required_vars if not os.getenv(var)]
    
    if missing:
        logging.error(f"Missing environment variables: {', '.join(missing)}")
    else:
        bot = BlueskyBot()
        bot.run()
