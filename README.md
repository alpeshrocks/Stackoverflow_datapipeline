# Stack Overflow Data Pipeline

The Stack Overflow Data Pipeline is a Python script that fetches data from the Stack Overflow API and saves it into separate CSV files. The script retrieves data for questions, posts, users, tags, and comments, and transforms the date fields from seconds format to 'dd-mm-yyyy' format before storing them in the CSV files.
We can run complex business queries on this data by filtering it according to the requirements in any Data Analysis tool or in Python code.
Most of the API's are open and don't require authentication.
## Installation

1. Clone the repository:

   ```shell
   https://github.com/alpeshrocks/Stackoverflow_datapipeline.git
   ```
2. Setup
    ```shell
    pip install -r requirements.txt
   ```
## Usage

    python data_pipeline.py --output-dir <output_directory>

 Replace <output_directory> with the desired directory where the CSV files will be saved.
 If not provided, the default output directory is set to output.
 The script will fetch data from the Stack Overflow API and save it into separate CSV files in the specified output directory.

## Approach
The Stack Overflow Data Pipeline follows these steps to fetch and save the data:

1. Fetch Data: The script makes HTTP requests to the Stack Overflow API to retrieve data for questions, posts, users, tags, and comments. Each API endpoint is called with the appropriate parameters (e.g., sorting, ordering) tofetch the desired data.

2. Transform Dates: The script identifies fields ending with _date in the retrieved data and converts them from seconds format to 'dd-mm-yyyy' format. The datetime module is used for the conversion.

3. Save Data: The transformed data is then saved into separate CSV files. Each CSV file corresponds to a specific type of data (questions, posts, users, tags, comments). The CSV files are named as stackoverflow_<data_type>.csv and stored in the specified output directory.

4. Logging: The script uses the logging module to provide logging functionality. Log messages are written to both a log file (pipeline.log) and displayed on the console. Information about the progress and status of the data pipeline execution is logged.

## File Structure
The file structure of the project is as follows:
```
├── data_pipeline.py        # Main script to run the data pipeline
├── requirements.txt        # Dependencies required by the script
├── pipeline.log            # Log file to store the execution logs
├── README.md               # Documentation explaining the code and approach
└── output/                 # Output directory (can be customized)
    ├── stackoverflow_questions.csv    # CSV file for questions data
    ├── stackoverflow_posts.csv        # CSV file for posts data
    ├── stackoverflow_users.csv        # CSV file for users data
    ├── stackoverflow_tags.csv         # CSV file for tags data
    └── stackoverflow_comments.csv     # CSV file for comments data
```
## Author
Alpesh Vijay Shinde | [mail](alpeshshinde29@gmail.com)
