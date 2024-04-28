# sql-mts-k

Konzipiert für die Auswertung mit Grafana

## Dependencies

```
pip install -r requirements.txt
```

## Database

### Table: Stations

| *id* | name        | brand      | street     | place | lat | lng | dist                      | houseNumber | postCode |
|------|-------------|------------|------------|-------|-----|-----|---------------------------|-------------|----------|
| ID   | Unique Name | Shell|ARAL | Musterstr. | Stadt | 0.0 | 0.0 | km from lat lng in config | 1           | 12345    |

### Table: Status

| *stationId* | isOpen     |
|-------------|------------|
| Station ID  | Is it open |

### Table: Prices

| *stationId* | *timestamp*  | price                  |
|-------------|--------------|------------------------|
| Station ID  | 1234567890   | how much at timestamp? |

## Sample queries

Günstigste Tankstelle im Umkreis unter Betrachtung der Fahrtkosten zur Tankstelle:
```
SELECT name, price, price*60 + (price*2*dist*8)/100 as total
FROM (
    SELECT * FROM prices
    INNER JOIN stations
    ON stations.id == prices.stationId
    ORDER BY timestamp DESC
)
GROUP BY name ORDER BY total;
```
