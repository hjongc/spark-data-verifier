# Spark Data Verifier

[English](#english) | [한국어](#korean)

---

## English

Enterprise-grade data verification tool for ensuring data consistency during platform migrations. Developed and used in production for a real data platform migration project between Hive/Spark-based databases.

### Features

- **Dual Verification Modes**
  - **FAST Mode**: SHA hashing + FULL OUTER JOIN - resource-efficient, faster execution
  - **DETAILED Mode**: EXCEPT operator - shows exact differences, comprehensive analysis

- **Parallel Processing**: Automatically handles partitioned tables with configurable parallelism
- **Connection Pooling**: Efficient database connection management with Apache DBCP2
- **Retry Logic**: Automatic retry with exponential backoff for transient failures
- **Result Persistence**: Stores verification results in MySQL for auditing and tracking
- **Environment-Based Configuration**: Secure credential management via environment variables
- **Comprehensive Logging**: SLF4J/Logback with file and console outputs

### Architecture

```
├── config/              # Configuration management
├── model/               # Domain models and enums
├── strategy/            # Verification strategy implementations
├── service/             # Business logic layer
├── repository/          # Data persistence layer
└── util/                # Utility classes
```

### Prerequisites

- Java 11 or higher
- Maven 3.6+
- Apache Hive/Spark with JDBC access
- MySQL 8.0+ (for result storage)

### Quick Start

#### 1. Build

```bash
mvn clean package
```

This creates `target/spark-data-verifier-1.0.0.jar` with all dependencies included.

#### 2. Configure Environment Variables

```bash
export HIVE_JDBC_URL="jdbc:hive2://your-host:10009/default?kyuubi.engine.type=SPARK_SQL"
export HIVE_USERNAME="spark"
export HIVE_PASSWORD=""

export MYSQL_JDBC_URL="jdbc:mysql://your-mysql-host:3306/verification_db"
export MYSQL_USERNAME="your_user"
export MYSQL_PASSWORD="your_password"
```

#### 3. Run Verification

**Fast Mode (Default)**
```bash
java -jar target/spark-data-verifier-1.0.0.jar \
  -t users \
  -d source_database \
  -a target_database \
  -o 20250115 \
  -m migration_batch_001
```

**Detailed Mode with Filters**
```bash
java -jar target/spark-data-verifier-1.0.0.jar \
  -t transactions \
  -d source_database \
  -a target_database \
  -o 20250115 \
  -m migration_batch_001 \
  --mode DETAILED \
  -e "updated_at,audit_timestamp" \
  -w "transaction_date >= '2025-01-01'"
```

### Command Line Options

| Option | Long Form | Required | Description |
|--------|-----------|----------|-------------|
| `-t` | `--tableName` | Yes | Table name to verify |
| `-d` | `--baseDatabase` | Yes | Base (source) database name |
| `-a` | `--targetDatabase` | Yes | Target (destination) database name |
| `-o` | `--odate` | Yes | Operation date (YYYYMMDD) |
| `-m` | `--mid` | Yes | Migration ID or batch identifier |
| `-w` | `--whereCondition` | No | WHERE clause condition (default: `1=1`) |
| `-e` | `--excludeColumns` | No | Comma-separated columns to exclude |
| `--mode` | `--mode` | No | Verification mode: `FAST` or `DETAILED` (default: `FAST`) |

### Verification Modes Comparison

#### FAST Mode (SHA + FULL OUTER JOIN)

```sql
SELECT * FROM base a
FULL OUTER JOIN target b
ON sha(CONCAT_WS('', a.col1, a.col2, ...)) = sha(CONCAT_WS('', b.col1, b.col2, ...))
WHERE a.first_col IS NULL OR b.first_col IS NULL
```

**Pros:**
- ✅ Faster execution time
- ✅ Lower Spark resource usage
- ✅ Single JOIN operation
- ✅ Ideal for large-scale batch verification

**Cons:**
- ❌ Only shows if differences exist, not exact values
- ❌ Cannot identify which specific columns differ

**Use When:**
- Quick verification for many tables
- Resource constraints
- Only need to know if data matches

#### DETAILED Mode (EXCEPT)

```sql
SELECT col1, col2, ... FROM base
EXCEPT
SELECT col1, col2, ... FROM target
```

**Pros:**
- ✅ Shows exact differing values
- ✅ Identifies specific rows and columns
- ✅ Comprehensive analysis

**Cons:**
- ❌ Slower execution
- ❌ Higher resource consumption
- ❌ Requires sorting and full comparison

**Use When:**
- Need to see exact differences
- Debugging data issues
- Detailed reconciliation required

### Configuration

#### application.yml

Located at `src/main/resources/application.yml`:

```yaml
database:
  hive:
    jdbcUrl: jdbc:hive2://localhost:10009/default
    username: spark
    password: ""
    driverClassName: org.apache.hive.jdbc.HiveDriver

  mysql:
    jdbcUrl: jdbc:mysql://localhost:3306/verification_db
    username: user
    password: password
    driverClassName: com.mysql.cj.jdbc.Driver

verification:
  maxParallelPartitions: 100
  sampleLimit: 5
  jdbcFetchSize: 20000
  retryAttempts: 3
  retryDelayMs: 1000
```

#### Environment Variables (Recommended for Production)

All configuration values can be overridden with environment variables:

```bash
HIVE_JDBC_URL
HIVE_USERNAME
HIVE_PASSWORD
MYSQL_JDBC_URL
MYSQL_USERNAME
MYSQL_PASSWORD
VERIFICATION_MAX_PARALLEL
VERIFICATION_SAMPLE_LIMIT
```

### MySQL Result Table Schema

The tool automatically creates this table if it doesn't exist:

```sql
CREATE TABLE verification_result (
  id BIGINT AUTO_INCREMENT PRIMARY KEY,
  table_name VARCHAR(255) NOT NULL,
  base_database_name VARCHAR(255) NOT NULL,
  target_database_name VARCHAR(255) NOT NULL,
  partition_key VARCHAR(500),
  execution_status VARCHAR(50) NOT NULL,
  sample_data TEXT,
  start_time DATETIME NOT NULL,
  end_time DATETIME,
  processing_time_ms BIGINT,
  base_row_count BIGINT,
  target_row_count BIGINT,
  differences_found BIGINT,
  where_condition TEXT,
  verification_mode VARCHAR(50),
  odate VARCHAR(50),
  mid VARCHAR(100),
  created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
  INDEX idx_table_odate (table_name, odate),
  INDEX idx_status (execution_status),
  INDEX idx_created (created_at)
);
```

### Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success - All data matches |
| 1 | Differences found |
| 2 | Errors occurred during verification |

### Real-World Usage Example

This tool was used in a production data platform migration project:

```bash
#!/bin/bash
# verify_migration.sh - Batch verification script

TABLES=(
  "customer_info"
  "transaction_history"
  "product_catalog"
  "order_details"
)

ODATE=$(date +%Y%m%d)
MID="migration_${ODATE}"

for TABLE in "${TABLES[@]}"; do
  echo "Verifying $TABLE..."

  java -jar spark-data-verifier-1.0.0.jar \
    -t "$TABLE" \
    -d source_database \
    -a target_database \
    -o "$ODATE" \
    -m "$MID" \
    --mode FAST \
    -e "updated_at,created_at"

  EXIT_CODE=$?

  if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ $TABLE verification passed"
  elif [ $EXIT_CODE -eq 1 ]; then
    echo "⚠ $TABLE has differences - running detailed verification"

    java -jar spark-data-verifier-1.0.0.jar \
      -t "$TABLE" \
      -d source_database \
      -a target_database \
      -o "$ODATE" \
      -m "${MID}_detailed" \
      --mode DETAILED \
      -e "updated_at,created_at"
  else
    echo "✗ $TABLE verification failed"
    exit 1
  fi
done

echo "All verifications completed"
```

### Performance Tips

1. **Partitioned Tables**: Automatically processed in parallel (up to 100 partitions by default)
2. **Large Tables**: Use FAST mode first, then DETAILED mode only if differences found
3. **Column Exclusions**: Exclude timestamp columns that may differ due to migration timing
4. **JDBC Fetch Size**: Default 20000 rows per fetch - adjust based on network and memory
5. **Connection Pooling**: Automatically managed - reuses connections efficiently

### Monitoring and Debugging

Check logs in `logs/verification.log`:

```bash
tail -f logs/verification.log
```

Query results from MySQL:

```sql
-- Check verification status
SELECT table_name, execution_status, processing_time_ms, differences_found
FROM verification_result
WHERE odate = '20250115'
ORDER BY created_at DESC;

-- Find failed verifications
SELECT table_name, partition_key, sample_data
FROM verification_result
WHERE execution_status = 'NOT_OK' AND odate = '20250115';
```

### Development

#### Run Tests

```bash
mvn test
```

#### Build Without Tests

```bash
mvn clean package -DskipTests
```

#### Code Coverage

```bash
mvn clean test jacoco:report
```

### Technology Stack

- **Java 11**: Base language
- **Apache Hive JDBC**: Spark/Hive connectivity
- **MySQL Connector**: Result persistence
- **Apache Commons DBCP2**: Connection pooling
- **SLF4J + Logback**: Logging framework
- **JUnit 5 + Mockito**: Testing framework
- **Maven**: Build and dependency management

### License

This project is part of a portfolio showcasing enterprise Java development skills.

### Author

Developed and used in production for data platform migration projects.

---

## Korean

<a name="korean"></a>

Hive/Spark 기반 데이터베이스 간 플랫폼 마이그레이션 시 데이터 일관성을 보장하기 위한 엔터프라이즈급 데이터 검증 도구입니다. 실제 데이터 플랫폼 이관 프로젝트에서 개발 및 사용되었습니다.

### 주요 기능

- **이중 검증 모드**
  - **FAST 모드**: SHA 해싱 + FULL OUTER JOIN - 리소스 효율적, 빠른 실행
  - **DETAILED 모드**: EXCEPT 연산자 - 정확한 차이점 표시, 포괄적 분석

- **병렬 처리**: 파티션 테이블 자동 처리 (병렬도 설정 가능)
- **커넥션 풀링**: Apache DBCP2 기반 효율적인 DB 연결 관리
- **재시도 로직**: 일시적 장애에 대한 자동 재시도 (exponential backoff)
- **결과 저장**: MySQL에 검증 결과 저장으로 감사 추적 가능
- **환경 기반 설정**: 환경변수를 통한 안전한 credential 관리
- **포괄적 로깅**: SLF4J/Logback 기반 파일 및 콘솔 로깅

### 아키텍처

```
├── config/              # 설정 관리
├── model/               # 도메인 모델 및 Enum
├── strategy/            # 검증 전략 구현
├── service/             # 비즈니스 로직 계층
├── repository/          # 데이터 영속화 계층
└── util/                # 유틸리티 클래스
```

### 사전 요구사항

- Java 11 이상
- Maven 3.6+
- Apache Hive/Spark (JDBC 접근 가능)
- MySQL 8.0+ (결과 저장용)

### 빠른 시작

#### 1. 빌드

```bash
mvn clean package
```

모든 의존성이 포함된 `target/spark-data-verifier-1.0.0.jar` 생성됩니다.

#### 2. 환경변수 설정

```bash
export HIVE_JDBC_URL="jdbc:hive2://your-host:10009/default?kyuubi.engine.type=SPARK_SQL"
export HIVE_USERNAME="spark"
export HIVE_PASSWORD=""

export MYSQL_JDBC_URL="jdbc:mysql://your-mysql-host:3306/verification_db"
export MYSQL_USERNAME="your_user"
export MYSQL_PASSWORD="your_password"
```

#### 3. 검증 실행

**FAST 모드 (기본값)**
```bash
java -jar target/spark-data-verifier-1.0.0.jar \
  -t users \
  -d source_database \
  -a target_database \
  -o 20250115 \
  -m migration_batch_001
```

**필터링을 사용한 DETAILED 모드**
```bash
java -jar target/spark-data-verifier-1.0.0.jar \
  -t transactions \
  -d source_database \
  -a target_database \
  -o 20250115 \
  -m migration_batch_001 \
  --mode DETAILED \
  -e "updated_at,audit_timestamp" \
  -w "transaction_date >= '2025-01-01'"
```

### 검증 모드 비교

#### FAST 모드 (SHA + FULL OUTER JOIN)

**장점:**
- ✅ 빠른 실행 시간
- ✅ 낮은 Spark 리소스 사용량
- ✅ 단일 JOIN 연산
- ✅ 대규모 배치 검증에 이상적

**단점:**
- ❌ 차이 존재 여부만 표시, 정확한 값은 미표시
- ❌ 어떤 컬럼이 다른지 식별 불가

**사용 시기:**
- 많은 테이블의 빠른 검증
- 리소스 제약이 있을 때
- 데이터 일치 여부만 확인하면 될 때

#### DETAILED 모드 (EXCEPT)

**장점:**
- ✅ 정확한 차이 값 표시
- ✅ 특정 행과 컬럼 식별
- ✅ 포괄적 분석

**단점:**
- ❌ 느린 실행 속도
- ❌ 높은 리소스 소비
- ❌ 정렬 및 전체 비교 필요

**사용 시기:**
- 정확한 차이점을 확인해야 할 때
- 데이터 이슈 디버깅
- 상세한 대사가 필요할 때

### 실제 사용 예시

프로덕션 데이터 플랫폼 마이그레이션 프로젝트에서 사용된 방식:

```bash
#!/bin/bash
# verify_migration.sh - 배치 검증 스크립트

TABLES=(
  "customer_info"
  "transaction_history"
  "product_catalog"
  "order_details"
)

ODATE=$(date +%Y%m%d)
MID="migration_${ODATE}"

for TABLE in "${TABLES[@]}"; do
  echo "Verifying $TABLE..."

  java -jar spark-data-verifier-1.0.0.jar \
    -t "$TABLE" \
    -d source_database \
    -a target_database \
    -o "$ODATE" \
    -m "$MID" \
    --mode FAST \
    -e "updated_at,created_at"

  EXIT_CODE=$?

  if [ $EXIT_CODE -eq 0 ]; then
    echo "✓ $TABLE 검증 통과"
  elif [ $EXIT_CODE -eq 1 ]; then
    echo "⚠ $TABLE 차이 발견 - 상세 검증 실행"

    java -jar spark-data-verifier-1.0.0.jar \
      -t "$TABLE" \
      -d source_database \
      -a target_database \
      -o "$ODATE" \
      -m "${MID}_detailed" \
      --mode DETAILED \
      -e "updated_at,created_at"
  else
    echo "✗ $TABLE 검증 실패"
    exit 1
  fi
done

echo "모든 검증 완료"
```

### 성능 최적화 팁

1. **파티션 테이블**: 자동으로 병렬 처리 (기본값 최대 100개 파티션)
2. **대용량 테이블**: FAST 모드 먼저 실행 후, 차이가 있을 경우에만 DETAILED 모드 실행
3. **컬럼 제외**: 마이그레이션 타이밍으로 인해 차이날 수 있는 타임스탬프 컬럼 제외
4. **JDBC Fetch Size**: 기본값 20000 rows - 네트워크와 메모리 상황에 따라 조정
5. **커넥션 풀링**: 자동 관리 - 연결을 효율적으로 재사용

### 기술 스택

- **Java 11**: 기본 언어
- **Apache Hive JDBC**: Spark/Hive 연결
- **MySQL Connector**: 결과 저장
- **Apache Commons DBCP2**: 커넥션 풀링
- **SLF4J + Logback**: 로깅 프레임워크
- **JUnit 5 + Mockito**: 테스트 프레임워크
- **Maven**: 빌드 및 의존성 관리

### 저자

실제 데이터 플랫폼 이관 프로젝트에서 개발 및 사용되었습니다.
