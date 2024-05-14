# nfdump sender to OpenObserve

## Installation

Make sure that you have nfdump installed on your system.

## setup the environment

```bash
python3 -m venv nfo2

source nfo2/bin/activate

pip install -r requirements.txt

```

## Configuration

Make sure that you change the ENDPOINT_URL in the file app.py to the correct URL of your OpenObserve instance and URL and the credentials too.

## Running the script

```bash
python app.py
```

The script will now watch for new files in the input folder and will send the data to OpenObserve.

There are some sample files in the input1 folder. You can copy them to the input folder while the app.py is running to see the data being sent to OpenObserve.
