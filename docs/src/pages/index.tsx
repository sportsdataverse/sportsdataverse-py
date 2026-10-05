import type {ReactNode} from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import registry from '@site/src/data/leagues.json';
import styles from './styles.module.css';

// The R and Python packages these leagues mirror; they were the six feature cards' outbound links.
const SISTER_PACKAGES: {label: string; href: string}[] = [
  {label: 'hoopR', href: 'https://hoopR.sportsdataverse.org'},
  {label: 'wehoop', href: 'https://wehoop.sportsdataverse.org'},
  {label: 'cfbfastR', href: 'https://cfbfastR.sportsdataverse.org'},
  {label: 'nflverse', href: 'https://nflverse.nflverse.com'},
  {label: 'nflreadpy', href: 'https://github.com/nflverse/nflreadpy'},
  {label: 'baseballr', href: 'https://billpetti.github.io/baseballr/'},
  {label: 'fastRhockey', href: 'https://fastRhockey.sportsdataverse.org'},
];

function HomepageHeader(): ReactNode {
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
            Getting Started
          </Link>
          <Link
            className="button button--outline button--secondary button--lg"
            to="/docs/ecosystem">
            Ecosystem &amp; philosophy
          </Link>
        </div>
      </div>
    </header>
  );
}

// Every league the codegen documents, grouped by sport (docs/src/data/leagues.json, written by
// tools/codegen/generate.py); each chip opens the league's index page.
function LeagueGrid(): ReactNode {
  return (
    <section className={styles.leagues} aria-labelledby="leagues">
      <div className="container">
        <h2 id="leagues">Leagues</h2>
        {registry.sports.map((sport) => (
          <div key={sport.key} className={styles.sport}>
            <h3 className={styles.sportTitle}>
              <Link to={`/docs/leagues/${sport.key}`}>{sport.label}</Link>
            </h3>
            <ul className={styles.leagueList}>
              {sport.leagues.map((l) => (
                <li key={l.prefix}>
                  <Link className={styles.league} to={`/docs/${l.prefix}/`}>
                    {l.label}
                  </Link>
                </li>
              ))}
            </ul>
          </div>
        ))}
        <p className={styles.sisters}>
          Sister packages:{' '}
          {SISTER_PACKAGES.map((p, i) => (
            <span key={p.label}>
              {i > 0 && ' · '}
              <Link to={p.href}>{p.label}</Link>
            </span>
          ))}
        </p>
      </div>
    </section>
  );
}

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="The SportsDataverse's Python Package for Sports Data.">
      <HomepageHeader />
      <main>
        <LeagueGrid />
      </main>
    </Layout>
  );
}
