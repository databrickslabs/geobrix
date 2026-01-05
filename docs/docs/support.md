---
sidebar_position: 10
---

# Support

## Important Notice

Please note that all projects in the [/databrickslabs](https://github.com/databrickslabs) GitHub account are provided for your exploration only, and are **not formally supported by Databricks** with Service Level Agreements (SLAs). 

They are provided AS-IS and we do not make any guarantees of any kind. 

**Please do not submit a support ticket relating to any issues arising from the use of these projects.**

## Getting Help

### GitHub Issues

Any issues discovered through the use of this project should be filed as [GitHub Issues](https://github.com/databrickslabs/geobrix/issues) on the repository. 

They will be reviewed as time permits, but there are **no formal SLAs for support**.

### Before Filing an Issue

1. **Check Existing Issues**: Search [existing issues](https://github.com/databrickslabs/geobrix/issues) to see if your problem has already been reported
2. **Review Documentation**: Check this documentation and the [README](https://github.com/databrickslabs/geobrix/blob/main/README.md)
3. **Check Known Limitations**: Review [Known Limitations](./limitations.md) to see if it's a documented limitation

### Filing a Good Issue

When filing an issue, please include:

#### Environment Information
- Databricks Runtime version (e.g., DBR 17.3 LTS)
- Cluster configuration (node types, number of workers)
- GeoBrix version
- Python/Scala version

#### Problem Description
- Clear description of the issue
- Expected behavior
- Actual behavior
- Error messages (full stack traces if available)

#### Reproduction Steps
- Minimal code to reproduce the issue
- Sample data if possible (or description of data)
- Steps to reproduce

#### Example

```markdown
**Environment:**
- DBR: 17.3 LTS
- Cluster: 2 workers, Standard_DS3_v2
- GeoBrix: 0.1.0-beta
- Python: 3.12

**Issue:**
Getting error when reading large GeoTIFF files.

**Error Message:**
```
java.lang.OutOfMemoryError: Java heap space
```

**Reproduction:**
```python
df = spark.read.format("gdal").load("/data/large.tif")
df.count()
```

**Expected:**
Should read file successfully.

**Actual:**
OOM error on executors.
```

## Community Resources

### GitHub Discussions

For general questions, discussions, and sharing use cases, consider using [GitHub Discussions](https://github.com/databrickslabs/geobrix/discussions) if available.

### Databricks Community

- [Databricks Community Forums](https://community.databricks.com/)
- Look for topics related to spatial processing and GeoBrix

## Contributing

Contributions are welcome! If you'd like to contribute:

1. Fork the repository
2. Create a feature branch
3. Make your changes
4. Submit a pull request

See [CONTRIBUTING.md](https://github.com/databrickslabs/geobrix/blob/main/CONTRIBUTING.md) for more details.

## Feature Requests

Feature requests can be filed as GitHub Issues with the label "enhancement". Please provide:

- Clear description of the feature
- Use case(s) for the feature
- Examples of how it would be used
- Any relevant references or similar implementations

## Documentation Improvements

Found an error in the documentation or have a suggestion for improvement?

- Documentation issues can be filed on [GitHub Issues](https://github.com/databrickslabs/geobrix/issues)
- Pull requests for documentation improvements are welcome
- Tag with "documentation" label

## Related Projects

### DBLabs Mosaic (Maintenance Mode)

If you're using DBLabs Mosaic (DBR 13.3 LTS):
- [Mosaic Documentation](https://databrickslabs.github.io/mosaic/index.html)
- [Mosaic GitHub](https://github.com/databrickslabs/mosaic)

**Note**: Mosaic is in maintenance mode and will be retired with [DBR 13.3 EoS](https://docs.databricks.com/aws/en/release-notes/runtime/#supported-databricks-runtime-lts-releases) in August 2026.

### Databricks Built-in Spatial Functions

For general spatial operations, consider using:
- [Databricks Spatial SQL Functions](https://docs.databricks.com/aws/en/sql/language-manual/sql-ref-st-geospatial-functions) (DBR 17.1+)
- [GEOMETRY Type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/geometry-type)
- [GEOGRAPHY Type](https://docs.databricks.com/aws/en/sql/language-manual/data-types/geography-type)

GeoBrix is designed to augment these built-in capabilities, particularly for raster processing and specialized grid systems.

## Databricks Labs

Learn more about Databricks Labs projects:
- [Databricks Labs](https://www.databricks.com/learn/labs)
- Databricks Labs provides pre-built solutions and reference architectures
- All Labs projects are provided AS-IS without formal support

## License

GeoBrix is provided under the license specified in the [LICENSE](https://github.com/databrickslabs/geobrix/blob/main/LICENSE) file.

## Acknowledgments

GeoBrix builds upon the work of:
- [GDAL/OGR](https://gdal.org/) - Geospatial Data Abstraction Library
- [Apache Spark](https://spark.apache.org/) - Unified analytics engine
- [DBLabs Mosaic](https://databrickslabs.github.io/mosaic/) - Predecessor project

## Contact

For questions about Databricks Labs:
- Visit [Databricks Labs](https://www.databricks.com/learn/labs)
- Contact through official Databricks channels

**Remember**: For GeoBrix-specific issues, please use [GitHub Issues](https://github.com/databrickslabs/geobrix/issues), not Databricks support channels.

## Useful Links

- [GeoBrix GitHub Repository](https://github.com/databrickslabs/geobrix)
- [GeoBrix Issues](https://github.com/databrickslabs/geobrix/issues)
- [Databricks Documentation](https://docs.databricks.com/)
- [Databricks Community](https://community.databricks.com/)

## Next Steps

- [Back to Introduction](./intro.md)
- [Installation Guide](./installation.md)
- [Quick Start](./quick-start.md)
- [Known Limitations](./limitations.md)

