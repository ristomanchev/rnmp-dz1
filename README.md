# РНМП, Домашна задача 1: квалитет на воздухот со Apache Flink

**Ристо Манчев, 231007** · Вовед во рударење на масивни податоци, ФИНКИ

Мерењата на граѓанските сензори за честички во воздухот на [Sensor.Community](https://sensor.community)
(тип PMS5003, септември 2026) се пуштаат како поток во Kafka. Flink апликацијата ги обработува
посебно за секој сензор (клуч `sensorId`) во два модула:

- **аналитички модул**: tumbling прозорци од 15 минути во event time, со просек и максимум на PM2.5 и PM10, во topic `aq-stats`;
- **event-driven модул**: аларми со состојба и тајмери (постојано загадување, сензор што замолкнал), во topic `aq-alerts`.

Доцните мерења одат во `aq-late`. Dashboard-от (бонус) ги прикажува резултатите во живо.
Документацијата е во [`docs/dokumentacija.pdf`](docs/dokumentacija.pdf).

## Структура

```
rnmp-dz1/
├── producer/          Python producer (Kafka) и анализа на податоците
├── flink-app/         Flink 2.3 апликација (Maven проект, Java 17)
├── dashboard/         Streamlit dashboard (бонус)
├── results/           скрипта за споредба и резултати од експериментот
├── docs/              документација (PDF)
├── create_topic.sh    го креира влезниот topic со 4 партиции
├── experiment.sh      експеримент earliest наспроти latest
└── data/              податоци (не се во репото, види чекор 2)
```

## Потребно

| Алатка | Тестирано со |
|---|---|
| Docker (Docker Desktop) | 29.1 |
| JDK 17 | OpenJDK 17.0.18 |
| Maven | 3.10.1 |
| Python | 3.12 (потребно е 3.11+, поради Streamlit) |
| Git, curl | |

Командите се за macOS и Linux (на Windows: WSL или Git Bash). На macOS скриптите сами ја наоѓаат
Java 17. На Linux `JAVA_HOME` треба да покажува на JDK 17.

## 1. Kafka

Kafka се стартува од репото на предметот:

```sh
git clone https://github.com/stefanandonov/RNMP_homework1.git
cd RNMP_homework1
docker compose up -d --wait        # Kafka на localhost:9092
```

## 2. Податоци

Од папката `rnmp-dz1`:

```sh
mkdir -p data
curl -L -o data/2026-09_pms5003.zip \
  https://archive.sensor.community/csv_per_month/2026-09/2026-09_pms5003.zip
```

Датотеката не треба да се распакува: producer-от го чита CSV-то директно од zip-от.

## 3. Python околина

```sh
python3 -m venv .venv
.venv/bin/pip install -r producer/requirements.txt -r dashboard/requirements.txt
```

Опционално, анализа на податоците (колку често доаѓаат мерењата, перцентили на PM2.5):

```sh
.venv/bin/python producer/explore.py --hours 24
```

## 4. Влезен topic

```sh
./create_topic.sh
```

Го креира `air-quality` со 4 партиции. Се извршува **пред** Flink апликацијата.

## 5. Flink апликација

```sh
cd flink-app
./run-job.sh --offsets latest
```

`run-job.sh` го компајлира проектот со Maven и ја стартува апликацијата со обична `java` команда
(локален Flink mini-cluster). Апликацијата работи додека не се прекине со `Ctrl+C`. Откако ќе се
испечати `Start: ...`, почекајте ~10 секунди пред да го стартувате producer-от.

| Аргумент | Default | Значење |
|---|---|---|
| `--offsets` | `latest` | `earliest` = чита сè што е во topic-от; `latest` = само пораките што пристигнуваат по стартот |
| `--ooo-sec` | `180` | bound на watermark-от, во секунди време на податоците |
| `--run` | празно | име на извршувањето; излезните topics стануваат `aq-stats-<име>`, `aq-alerts-<име>`, `aq-late-<име>` |
| `--window-min` | `15` | големина на прозорецот (минути) |
| `--pm25-threshold` | `25` | праг за PM2.5 (µg/m³) |
| `--sustain-min` | `30` | минути над прагот за `POLLUTION_ALARM` |
| `--offline-min` | `15` | минути без мерење за `SENSOR_OFFLINE` |
| `--parallelism` | `4` | паралелизам (= број на партиции на влезниот topic) |
| `--print` | `true` | печатење на резултатите и во конзолата |
| `--bootstrap` | `localhost:9092` | адреса на Kafka |
| `--input-topic` | `air-quality` | влезен topic |

## 6. Producer

Во друг терминал, од папката `rnmp-dz1`:

```sh
.venv/bin/python producer/producer.py --speedup 60 --hours 6
```

| Аргумент | Default | Значење |
|---|---|---|
| `--speedup` | `60` | забрзување: 60 = еден час податоци за една минута; 0 = најбрзо што може |
| `--hours` | `6` | колку часови податоци да се пратат (од почетокот на месецот) |
| `--ooo-ratio` | `0.05` | дел од пораките што се праќаат со доцнење |
| `--max-delay-sec` | `180` | најголемо доцнење, во секунди време на податоците |
| `--seed` | `42` | ист seed = исти пораки по ист редослед |
| `--dry-run` | | само ги печати пораките, не праќа во Kafka |
| `--file`, `--topic`, `--bootstrap` | | датотека, topic (`air-quality`) и адреса на Kafka |

На крајот producer-от печати колку пораки пратил, колку биле одложени и колкав бил најголемиот неред.

## 7. Резултати

Од папката `RNMP_homework1`:

```sh
./run.sh consume_messages.py aq-stats
./run.sh consume_messages.py aq-alerts
```

`air-quality` (влез, Kafka клуч = `sensorId`):

```json
{"sensorId": "98124", "timestamp": 1788220800000, "pm10": 0.0, "pm25": 0.0, "lat": 49.422, "lon": 8.398}
```

`aq-stats` (еден прозорец за еден сензор):

```json
{"sensorId":"89417","windowStart":1788220800000,"windowEnd":1788221700000,"windowStartIso":"2026-09-01T00:00:00Z","count":6,"avgPm25":8.25,"maxPm25":12.00,"avgPm10":10.83,"maxPm10":13.00,"lat":49.82556,"lon":73.12215,"emittedAt":1794960531846}
```

`aq-alerts` (`type` е `POLLUTION_ALARM`, `ALARM_CLEARED`, `SENSOR_OFFLINE` или `SENSOR_BACK_ONLINE`):

```json
{"type":"POLLUTION_ALARM","sensorId":"50823","eventTime":1788222602000,"eventTimeIso":"2026-09-01T00:30:02Z","since":1788220802000,"pm25":56.90,"threshold":25.0,"lat":50.91090,"lon":20.66410,"emittedAt":1794960546793}
```

`aq-late` (мерења што стигнале откако нивниот прозорец веќе бил емитуван):

```json
{"sensorId":"86514","timestamp":1788221545000,"pm25":1.00}
```

Сите времиња се во милисекунди од 1970 (UTC). `emittedAt` е реалното време кога Flink го емитувал резултатот.

## 8. Dashboard (бонус)

```sh
.venv/bin/streamlit run dashboard/dashboard.py
```

Се отвора на http://localhost:8501 и се освежува на секои 2 секунди. Во левата лента се избира
извршувањето (вредноста на `--run`, празно за `aq-stats` и `aq-alerts`) и дали topics да се читаат
од почеток. За приказ во живо: прво dashboard-от, потоа Flink апликацијата, на крајот producer-от.

## 9. Експеримент: earliest наспроти latest

```sh
./experiment.sh                  # 6 часа податоци со 60x, ~8 минути
SPEEDUP=600 ./experiment.sh      # побрза проба, ~3 минути
```

Скриптата:

1. го брише и повторно го креира `air-quality`, заедно со излезите од претходен експеримент;
2. стартува три апликации со `--offsets latest` (bound 180, 60 и 0 s) **пред** producer-от;
3. пушта 6 часа податоци;
4. стартува две апликации со `--offsets earliest` (bound 180 и 0 s) врз истите податоци;
5. ги споредува резултатите со `results/compare.py` и ги зачувува во `results/summary.txt`.

Резултат со 60x:

| Извршување | Прозорци | Аларми | Доцни | Траење |
|---|---|---|---|---|
| latest, bound 180 s | 4032 | 86 | 0 | 339,6 s |
| earliest, bound 180 s | 4032 | 86 | 0 | 0,5 s |
| latest, bound 60 s | 4032 | 90 | 49 | 339,6 s |
| latest, bound 0 s | 4032 | 99 | 140 | 340,3 s |
| earliest, bound 0 s | 4032 | 86 | 0 | 0,3 s |

Со bound 180 s прозорците, алармите и доцните мерења во latest и earliest се идентични, ред по ред.
Објаснувањето е во документацијата.

## Чести проблеми

| Проблем | Причина и решение |
|---|---|
| `mvn exec:java` паѓа со `ClassNotFoundException` | Flink 2.3 не работи со classloader-от на `exec:java`. Користете `./run-job.sh`. |
| Апликацијата работи, но нема резултати | Апликацијата со `latest` е стартувана после producer-от, или topic-от е креиран после неа. Првиот прозорец се затвора по ~18 минути податоци (18 s со 60x). |
| `earliest` дава доцни мерења и чудни аларми | Во topic-от има повеќе извршувања на producer-от, а секое почнува од 00:00. Испразнете го topic-от (`./experiment.sh` го прави тоа). |
| `NoBrokersAvailable` / грешка за Kafka | Kafka не работи: `docker compose up -d --wait` во `RNMP_homework1`. |
