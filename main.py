import requests
import csv
import logging
import argparse
from datetime import datetime


class StackOverflowDataPipeline:
    def __init__(self, output_dir):
        """
        Initializes the StackOverflowDataPipeline.

        Args:
            output_dir (str): The output directory for CSV files.
        """
        self.base_url = "https://api.stackexchange.com/2.3"
        self.output_dir = output_dir
        self.logger = self.setup_logger()

    def setup_logger(self):
        """
        Sets up the logger with the desired configuration.

        Returns:
            logging.Logger: Configured logger object.
        """
        logger = logging.getLogger("StackOverflowDataPipeline")
        logger.setLevel(logging.INFO)

        # Create a file handler that logs all messages to a file
        file_handler = logging.FileHandler("pipeline.log")
        file_handler.setLevel(logging.INFO)

        # Create a stream handler that logs INFO level messages to the console
        stream_handler = logging.StreamHandler()
        stream_handler.setLevel(logging.INFO)

        # Create a formatter for the log messages
        formatter = logging.Formatter("%(asctime)s - %(name)s - %(levelname)s - %(message)s")

        # Add the formatter to the handlers
        file_handler.setFormatter(formatter)
        stream_handler.setFormatter(formatter)

        # Add the handlers to the logger
        logger.addHandler(file_handler)
        logger.addHandler(stream_handler)

        return logger

    def fetch_data(self, endpoint, params):
        """
        Fetches data from the specified API endpoint.

        Args:
            endpoint (str): The URL endpoint for the API.
            params (dict): The query parameters for the API request.

        Returns:
            list: List of data items.
        """
        try:
            response = requests.get(endpoint, params=params)
            response.raise_for_status()  # Raises an exception for non-2xx status codes
            return response.json()["items"]
        except requests.exceptions.RequestException as e:
            self.logger.error(f"Failed to fetch data from {endpoint}. Error: {e}")
            return None

    def write_to_csv(self, data, csv_file):
        """
        Writes data to a CSV file.

        Args:
            data (list): The data to be written.
            csv_file (str): The path to the CSV file.
        """
        if data:
            fieldnames = data[0].keys()
            transformed_data = self.transform_dates(data)
            try:
                with open(csv_file, "w", newline="", encoding="utf-8") as file:
                    writer = csv.DictWriter(file, fieldnames=fieldnames, extrasaction='ignore')
                    writer.writeheader()
                    writer.writerows(transformed_data)
            except IOError as e:
                self.logger.error(f"Failed to write to CSV file: {csv_file}. Error: {e}")

    def transform_dates(self, data):
        """
        Transforms field values ending with '_date' from seconds format to 'dd-mm-yyyy' format.

        Args:
            data (list): The data to be transformed.

        Returns:
            list: Transformed data.
        """
        transformed_data = []
        for item in data:
            transformed_item = {}
            for key, value in item.items():
                if key.endswith("_date"):
                    transformed_item[key] = self.convert_date(value)
                else:
                    transformed_item[key] = value
            transformed_data.append(transformed_item)
        return transformed_data

    def convert_date(self, seconds):
        """
        Converts a date value from seconds format to 'dd-mm-yyyy' format.

        Args:
            seconds (int): The date value in seconds.

        Returns:
            str: Date value in 'dd-mm-yyyy' format.
        """
        if seconds:
            return datetime.utcfromtimestamp(int(seconds)).strftime("%d-%m-%Y")
        return ""

    def run_pipeline(self):
        """
        Runs the data pipeline to fetch and save Stack Overflow data.
        """
        self.logger.info("Starting the data pipeline.")

        questions = self.fetch_questions()
        if questions:
            self.logger.info("Fetched Stack Overflow questions successfully.")
            self.write_to_csv(questions, f"{self.output_dir}/stackoverflow_questions.csv")
        else:
            self.logger.warning("Failed to fetch Stack Overflow questions.")

        posts = self.fetch_posts()
        if posts:
            self.logger.info("Fetched Stack Overflow posts successfully.")
            self.write_to_csv(posts, f"{self.output_dir}/stackoverflow_posts.csv")
        else:
            self.logger.warning("Failed to fetch Stack Overflow posts.")

        users = self.fetch_users()
        if users:
            self.logger.info("Fetched Stack Overflow users successfully.")
            self.write_to_csv(users, f"{self.output_dir}/stackoverflow_users.csv")
        else:
            self.logger.warning("Failed to fetch Stack Overflow users.")

        tags = self.fetch_tags()
        if tags:
            self.logger.info("Fetched Stack Overflow tags successfully.")
            self.write_to_csv(tags, f"{self.output_dir}/stackoverflow_tags.csv")
        else:
            self.logger.warning("Failed to fetch Stack Overflow tags.")

        comments = self.fetch_comments()
        if comments:
            self.logger.info("Fetched Stack Overflow comments successfully.")
            self.write_to_csv(comments, f"{self.output_dir}/stackoverflow_comments.csv")
        else:
            self.logger.warning("Failed to fetch Stack Overflow comments.")

        self.logger.info("Data saved successfully in separate CSV files.")
        self.logger.info("Data pipeline completed.")

    def fetch_questions(self):
        """
        Fetches Stack Overflow questions from the API.

        Returns:
            list: List of question items.
        """
        params = {
            "site": "stackoverflow",
            "order": "desc",
            "sort": "votes"
        }
        return self.fetch_data(f"{self.base_url}/questions", params)

    def fetch_posts(self):
        """
        Fetches Stack Overflow posts from the API.

        Returns:
            list: List of post items.
        """
        params = {
            "site": "stackoverflow",
            "order": "desc",
            "sort": "votes"
        }
        return self.fetch_data(f"{self.base_url}/posts", params)

    def fetch_users(self):
        """
        Fetches Stack Overflow users from the API.

        Returns:
            list: List of user items.
        """
        params = {
            "site": "stackoverflow",
            "order": "desc",
            "sort": "reputation"
        }
        return self.fetch_data(f"{self.base_url}/users", params)

    def fetch_tags(self):
        """
        Fetches Stack Overflow tags from the API.

        Returns:
            list: List of tag items.
        """
        params = {
            "site": "stackoverflow",
            "order": "desc",
            "sort": "popular"
        }
        return self.fetch_data(f"{self.base_url}/tags", params)

    def fetch_comments(self):
        """
        Fetches Stack Overflow comments from the API.

        Returns:
            list: List of comment items.
        """
        params = {
            "site": "stackoverflow",
            "order": "desc",
            "sort": "votes"
        }
        return self.fetch_data(f"{self.base_url}/comments", params)


def parse_arguments():
    """
    Parses command-line arguments.

    Returns:
        argparse.Namespace: Parsed arguments.
    """
    parser = argparse.ArgumentParser(description="Stack Overflow DataPipeline")
    parser.add_argument("--output-dir", "-o", default="output", help="Output directory for CSV files")
    return parser.parse_args()


if __name__ == "__main__":
    args = parse_arguments()

    # Instantiate the StackOverflowDataPipeline class with the provided output directory
    pipeline = StackOverflowDataPipeline(args.output_dir)

    # Run the data pipeline
    pipeline.run_pipeline()
