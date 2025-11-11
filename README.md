# MKB Scraper

This project contains a small Python utility that downloads the full list of
MKB-10 (ICD-10) entries from [stetoskop.info](https://www.stetoskop.info/medjunarodna-klasifikacija-bolesti)
and stores them in a CSV file. The script was written for the Institute of
Public Health of Serbia "Dr Milan Jovanović Batut" to facilitate internal use of
the dataset.

> **Note:** Network access is disabled in the execution environment that built
> this repository, so the scraper could not be executed or validated from here.
> The code is ready to run in an environment with Internet access.

## Running locally

<<<<<<< ours
1. Ensure you have Python 3.9 or newer installed.
2. Install the dependencies:

   ```bash
   python -m venv .venv
=======
The project is intentionally lightweight so it can be executed on any desktop or
laptop without additional services.

1. **Install Python** – The scraper requires Python 3.9 or newer. On Windows you
   can download it from [python.org](https://www.python.org/downloads/); on
   Linux and macOS it is typically available through the system package manager.
2. **Create a virtual environment and install dependencies.** Open a terminal in
   the repository directory and run:

   ```bash
   python -m venv .venv
   # On Windows use: .\.venv\Scripts\activate
>>>>>>> theirs
   source .venv/bin/activate
   pip install -r requirements.txt
   ```

<<<<<<< ours
3. Run the scraper:
=======
   After activation the prompt should show `(.venv)` to indicate the isolated
   environment is active.
3. **Run the scraper** with the desired output file path:
>>>>>>> theirs

   ```bash
   python -m mkb_scrape -o mkb10.csv
   ```

<<<<<<< ours
   The output CSV contains the columns `code`, `description_serbian`, and
   `description_latin` separated by a pipe (`|`) character and sorted by code.
=======
   This downloads every category and diagnosis page from the MKB portal, sorts
   the entries by code, and saves a pipe-delimited CSV (`code|description_serbian|description_latin`).
4. (Optional) **Adjust runtime settings.** Run the help command to see
   additional flags such as `--delay` to throttle requests if needed:

   ```bash
   python -m mkb_scrape --help
   ```

   When you are done, deactivate the virtual environment with `deactivate`.
>>>>>>> theirs

## Running with Docker

Build the image and run it while mounting a local directory where the CSV should
be written:

```bash
docker build -t mkb-scraper .
docker run --rm -v "$(pwd)":/data mkb-scraper -o /data/mkb10.csv
```

Adjust the output path (`-o`) to match the mounted directory. The optional
`--delay` argument controls the pause in seconds between requests (default
`0.2`).
