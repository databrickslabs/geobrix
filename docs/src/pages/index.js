import React from 'react';
import clsx from 'clsx';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import Layout from '@theme/Layout';
import styles from './index.module.css';

function HomepageHeader() {
  const {siteConfig} = useDocusaurusContext();
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">{siteConfig.title}</h1>
        <p className="hero__subtitle">{siteConfig.tagline}</p>
        <div className={styles.buttons}>
          <Link
            className="button button--secondary button--lg"
            to="/docs/intro">
            Get Started →
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
            description="Process satellite imagery, elevation models, and gridded spatial data with GDAL-powered functions."
            link="/docs/packages/rasterx"
          />
          <Feature
            title="GridX"
            description="Spatial indexing with British National Grid (BNG) support for efficient location-based analysis."
            link="/docs/packages/gridx"
          />
          <Feature
            title="VectorX"
            description="Migrate legacy Mosaic geometries and work seamlessly with Databricks spatial types."
            link="/docs/packages/vectorx"
          />
        </div>
        <div className="row" style={{marginTop: '2rem'}}>
          <Feature
            title="Powerful Readers"
            description="Automatically registered Spark readers for Shapefile, GeoJSON, GeoPackage, GeoTIFF, and more."
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
        
        <section className={styles.quickStart}>
          <div className="container">
            <div className="row">
              <div className="col col--8 col--offset-2">
                <h2>Quick Start</h2>
                <p>Get up and running with GeoBrix in minutes:</p>
                <pre>
                  <code>
{`# Import and register functions
from databricks.labs.gbx.rasterx import functions as rx
rx.register(spark)

# Read and process geospatial data
rasters = spark.read.format("gdal").load("/data/rasters")
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

