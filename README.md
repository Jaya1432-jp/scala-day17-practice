# Scala Spark Day 17 - Window Functions

## Objective

Practice Spark Window Functions for ranking, partitioning, comparing previous and next records, and finding the latest record.

## Technologies

- Scala 2.13.16
- Apache Spark 3.5.6
- SBT 1.10.11
- Java 17
- Spark SQL

## Problem Statement

This project covers:

- `row_number`
- `rank`
- `dense_rank`
- `partitionBy`
- `orderBy`
- Top 3 students per course
- `lag`
- `lead`
- Latest record per student

## Project Structure

```text
scala-day17/
├── src/
│   └── main/
│       └── scala/
│           └── Day17.scala
├── students.csv
├── day17-result.txt
├── build.sbt
├── README.md
└── .gitignore
