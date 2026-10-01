# Kafka book streaming lab

The original `admin.py`, `producer.py`, and `consumer.py` are preserved. The new
scripts follow their configuration and polling style, using `confluent_kafka`
and a local Kafka broker at `localhost:9092`.

## Prerequisites and setup

- Docker (start Docker Desktop before running the Docker commands).
- Python 3 and a Python virtual environment.
- The `confluent-kafka` Python package.

Run all commands from the folder containing the scripts. In PowerShell:

```powershell
cd "C:\Users\halim\Downloads\Lab4 Kafka\Lab4 Kafka"
python -m venv .venv
.\.venv\Scripts\Activate.ps1
pip install confluent-kafka
```

If PowerShell blocks activation, use `.\.venv\Scripts\python.exe` instead of
`python` and `.\.venv\Scripts\python.exe -m pip` instead of `pip`.

## Book source

`book.txt` was downloaded automatically from Project Gutenberg:
[Alice's Adventures in Wonderland by Lewis Carroll](https://www.gutenberg.org/ebooks/11).
The [UTF-8 plain text download](https://www.gutenberg.org/ebooks/11.txt.utf-8)
is kept intact, including Gutenberg's header and license. Every line, including
blank lines and the Gutenberg information, is sent through Kafka.

## Run the lab

Open four terminals in the script folder. Prepare terminals 3 and 4 before
starting the consumer so the producer starts within its 30-second idle window.

### Terminal 1: Kafka

```powershell
docker pull apache/kafka-native:4.1.1
docker run -p 9092:9092 apache/kafka-native:4.1.1
```

Wait for Kafka to start. Keep this container running throughout the lab.

### Terminal 2: create the topic

```powershell
.\.venv\Scripts\Activate.ps1
python admin_book.py
```

This creates `book_topic` with one partition and replication factor one, waits
for the result, and lists the topics. An existing topic is accepted; other
Kafka errors are reported. Wait for this command to succeed before continuing.

### Terminal 3: start the consumer

```powershell
.\.venv\Scripts\Activate.ps1
python consumer_book.py
```

### Terminal 4: start the producer

```powershell
.\.venv\Scripts\Activate.ps1
python producer_book.py
```

The producer reads `book.txt` as UTF-8, sending each line as a separate message
and printing it. It polls delivery callbacks and flushes queued messages at the
end. Delivery failures are reported and cause an unsuccessful exit.

The consumer uses group `book_group` and `auto.offset.reset = smallest`.
It polls once per second, decodes UTF-8, converts to lowercase, replaces
characters other than English letters and whitespace with spaces, collapses
whitespace, and strips the result. This removes punctuation, digits, and special
characters; apostrophes and hyphens become word separators. For example,
`Hello, WORLD! 123` becomes `hello world`. Empty cleaned lines are skipped.
Each remaining line is printed and written as UTF-8 to `cleaned_book.txt`.

The consumer stops after 30 empty polls (about 30 seconds without messages),
or fails after five consecutive Kafka errors. Ctrl+C also stops it cleanly.
The file closes through a `with` block and the consumer closes in `finally`.
If it times out before you start the producer, restart it.

```text
book.txt
  -> producer_book.py
  -> Kafka topic "book_topic"
  -> consumer_book.py
  -> text cleaning
  -> cleaned_book.txt
```

## Output and repeated runs

`cleaned_book.txt` now contains 2,795 cleaned lines from a successful real Kafka run.
It is opened in append mode so a restart does not erase earlier output.
Kafka remembers committed offsets for `book_group`; `smallest` starts at the
beginning only when the group has no valid committed offset. Running the producer
again adds another copy of the book to the topic and, when consumed, the output.
For a fresh replay without republishing, stop the consumer, choose a new
`group.id` in `consumer_book.py`, and rename the old output file before restarting.
Run only one book consumer at a time. This simple lab uses the default automatic
offset commits; interrupted runs are not guaranteed to avoid duplicates or gaps.

## Files and official requirements

| Requirement | Implementation |
| --- | --- |
| 1. Examine the examples | All three original scripts were inspected and retained unchanged. |
| 2. Create a new topic | `admin_book.py` creates and lists `book_topic`. |
| 3. Download a Gutenberg book | `book.txt` contains the downloaded UTF-8 Alice book. |
| 4. Stream the book line by line | `producer_book.py` sends one Kafka message per line. |
| 5. Consume the new topic | `consumer_book.py` subscribes to `book_topic`. |
| 6. Clean and save incoming text | Built-in `re` cleans each received line and writes `cleaned_book.txt`. |

The original `index.md` and `lab.md` are also preserved.

## Validation performed

- Python syntax and UTF-8 checks passed for the new files.
- A local `.venv` was created and `confluent-kafka` installed.
- Real Kafka client imports passed.
- With in-memory Kafka substitutes, the producer sent all 3,762 source lines
  and flushed. The consumer produced 2,795 nonempty cleaned lines, tolerated
  temporary empty polls, stopped after 30 empty polls, and closed its resources.
- Simulated admin checks covered topic creation, an existing topic, and
  propagation of other Kafka errors.
- Docker was subsequently installed in Ubuntu-24.04 under WSL and the requested
  `apache/kafka-native:4.1.1` image was run in container `lab4-kafka`.
- Live topic creation and the existing-topic case passed. The actual producer
  and consumer both exited with code 0. All 3,762 messages were processed and
  the 2,795 output lines matched the expected cleaned book exactly, in order.
- The topic end offset and the committed `book_group` offset both equal 3,762,
  confirming the consumer reached the end and committed its progress.
- Logs are in `run_logs/admin.log`, `run_logs/producer_success.log`, and
  `run_logs/consumer_success.log`. The earlier `producer.log` and `consumer.log`
  record an unsuccessful attempt before WSL was kept active. IPv6 connection
  retries occurred during the successful run; the clients connected over IPv4.

## Relancer sur cette machine (Docker dans WSL)

Docker est installe dans Ubuntu WSL, pas dans Docker Desktop. Pour relancer
le conteneur existant depuis PowerShell, garder ce terminal ouvert :

```powershell
wsl -d Ubuntu-24.04 -u root -- docker start -a lab4-kafka
```

Ensuite, dans le dossier du projet, executer les scripts dans l'ordre indique
plus haut avec `.\.venv\Scripts\python.exe`. Le fichier de resultat est deja
rempli : relancer le producteur ajouterait une nouvelle copie du livre.
Pour arreter Kafka proprement depuis un autre terminal :

```powershell
wsl -d Ubuntu-24.04 -u root -- docker stop lab4-kafka
```
