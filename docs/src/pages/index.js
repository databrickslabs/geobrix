import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import Layout from '@theme/Layout';
import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <img
          src={useBaseUrl('/img/databricks-labs-logo.png')}
          alt="Databricks Labs"
          className={styles.heroLogo}
        />
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Get Started
          </Link>
        </div>
      </div>
    </header>
  );
}

function Feature({title, description, link}) {
  return (
    <div className={clsx('col col--4')}>
      <div className="text--center padding-horiz--md">
        <h3>{title}</h3>
        <p>{description}</p>
        {link && (
          <Link to={link}>
            Learn more →
          </Link>
        )}
      </div>
    </div>
  );
}

function HomepageFeatures() {
  return (
    <section className={styles.features}>
      <div className="container">
        <div className="row">
          <Feature
            title="RasterX"
            description="Satellite imagery, elevation models, and gridded data — reprojection, terrain analysis, spectral indices, XYZ/PMTiles tiling, and H3/quadbin aggregation. In large part, distributed rasterio + best-of-breed raster packages."
            link="/docs/api/raster-functions"
          />
          <Feature
            title="GridX"
            description="Discrete global grid indexing — British National Grid, CARTO quadbin, and custom user-defined grids: cell math, tessellation, and grid-aware aggregation."
            link="/docs/api/gridx-functions"
          />
          <Feature
            title="VectorX"
            description="Mapbox Vector Tile encoding, TIN elevation surfaces, and legacy Mosaic geometry migration to Databricks spatial types."
            link="/docs/api/vectorx-functions"
          />
        </div>
        <div className="row" style={{marginTop: '2rem'}}>
          <Feature
            title="Powerful Readers & Writers"
            description="Automatically registered Spark readers and writers for Shapefile, GeoJSON, GeoPackage, GeoTIFF, PMTiles, and more."
            link="/docs/readers/overview"
          />
          <Feature
            title="Multi-Language APIs"
            description="Native Scala implementation with Python and SQL bindings for maximum flexibility."
            link="/docs/api/overview"
          />
          <Feature
            title="Databricks Native"
            description="Built exclusively for Databricks Runtime, integrated with product spatial functions."
            link="/docs/installation"
          />
        </div>
      </div>
    </section>
  );
}

export default function Home() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={`${siteConfig.title} Documentation`}
      description="High-performance spatial processing library for Databricks">
      <HomepageHeader />
      <main>
        <HomepageFeatures />
        
        <section className={styles.tierCallout}>
          <div className="container">
            <div className="row">
              <div className="col col--8 col--offset-2 text--center">
                <h2>Start lightweight — the recommended raster tier</h2>
                <p>
                  The lightweight tier (pyrx) runs the full GeoBrix raster API on pure Python + rasterio:
                  a single wheel, no JAR and no init script, and it works everywhere — serverless,
                  standard/shared clusters, ARM, and Lakeflow declarative pipelines. VectorX is
                  likewise available lightweight (pyvx), as is all of GridX — BNG, quadbin, and
                  custom grids (pygx). The heavyweight Scala/GDAL tier is there when you need full
                  GDAL/OGR.
                </p>
                <Link
                  className="button button--primary button--md"
                  to="/docs/api/execution-tiers">
                  Compare Execution Tiers →
                </Link>
              </div>
            </div>
          </div>
        </section>

        <section className={styles.quickStart}>
          <div className="container">
            <div className="row">
              <div className="col col--8 col--offset-2">
                <h2>Quick Start</h2>
                <p>Get up and running with GeoBrix in minutes:</p>
                <pre>
                  <code>
{`# Install the lightweight wheel (single library, no JAR, no GDAL)
# Stage the wheel on a Unity Catalog Volume, then install the quoted PEP 508 named form:
%pip install --quiet "geobrix[light] @ file:///Volumes/<catalog>/<schema>/<volume>/geobrix-<version>-py3-none-any.whl"

# Import and register functions
from databricks.labs.gbx.pyrx import functions as rx
rx.register(spark)

# Read and process geospatial data
rasters = (spark.read.format("binaryFile").load("/data/rasters")
           .select(rx.rst_fromcontent("content").alias("tile")))
metadata = rasters.select(
    rx.rst_boundingbox("tile").alias("bbox"),
    rx.rst_metadata("tile").alias("metadata")
)`}
                  </code>
                </pre>
                <div style={{textAlign: 'center', marginTop: '1rem'}}>
                  <Link
                    className="button button--primary button--lg"
                    to="/docs/quick-start">
                    View Quick Start Guide
                  </Link>
                </div>
              </div>
            </div>
          </div>
        </section>

        <section className={styles.cta}>
          <div className="container">
            <div className="row">
              <div className="col col--12 text--center">
                <h2>Ready to get started?</h2>
                <p>Install GeoBrix on your Databricks cluster and unlock powerful geospatial capabilities.</p>
                <Link
                  className="button button--secondary button--lg"
                  to="/docs/installation">
                  Installation Guide
                </Link>
              </div>
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}

