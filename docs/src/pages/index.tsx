import type {ReactNode} from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import CodeBlock from '@theme/CodeBlock';
import registry from '@site/src/data/leagues.json';
import providerRegistry from '@site/src/data/providers.json';
import styles from './styles.module.css';

// The R and Python packages these leagues mirror.
const SISTER_PACKAGES: {label: string; href: string}[] = [
  {label: 'hoopR', href: 'https://hoopR.sportsdataverse.org'},
  {label: 'wehoop', href: 'https://wehoop.sportsdataverse.org'},
  {label: 'cfbfastR', href: 'https://cfbfastR.sportsdataverse.org'},
  {label: 'nflverse', href: 'https://nflverse.nflverse.com'},
  {label: 'nflreadpy', href: 'https://github.com/nflverse/nflreadpy'},
  {label: 'baseballr', href: 'https://baseballr.sportsdataverse.org'},
  {label: 'fastRhockey', href: 'https://fastRhockey.sportsdataverse.org'},
];

// Written by tools/codegen/generate.py from tools/codegen/sources.yaml: every provider the Getting Started
// sources table names, by sport. The strip shows the ones flagged `featured` (each once, in sport order);
// the rest sit behind "+N more".
type Provider = {key: string; label: string; href: string; featured?: boolean};
const FEATURED: Provider[] = [
  ...new Map(
    providerRegistry.sports
      .flatMap((s) => s.providers as Provider[])
      .filter((p) => p.featured)
      .map((p) => [p.key, p] as const),
  ).values(),
];

const EXAMPLE = `from sportsdataverse.nba import espn_nba_scoreboard

df = espn_nba_scoreboard()          # a polars DataFrame
df.select(["game_id", "home_name", "away_name", "home_score", "away_score"])`;

function HomepageHeader(): ReactNode {
  return (
    <header className={clsx('hero hero--primary', styles.heroBanner)}>
      <div className="container">
        <h1 className="hero__title">sportsdataverse-py</h1>
        <p className="hero__subtitle">
          Tidy play-by-play, box scores, schedules and rosters for 100+ leagues, as polars or pandas
          DataFrames.
        </p>
        <div className={styles.buttons}>
          <Link className="button button--secondary button--lg" to="/docs/intro">
            Getting started
          </Link>
          <Link className="button button--outline button--secondary button--lg" to="/docs/ecosystem">
            Ecosystem &amp; philosophy
          </Link>
        </div>
      </div>
    </header>
  );
}

function QuickStart(): ReactNode {
  return (
    <section className={styles.quickstart} aria-labelledby="quickstart">
      <div className="container">
        <h2 id="quickstart">Quickstart</h2>
        <div className="row">
          <div className="col col--5">
            <CodeBlock language="bash">pip install --pre sportsdataverse</CodeBlock>
            <p className={styles.note}>
              <code>--pre</code> installs the 0.2.0 beta, which these docs describe. See{' '}
              <Link to="/docs/guides/migration-0-2-0">Migrating to 0.2.0</Link>.
            </p>
          </div>
          <div className="col col--7">
            <CodeBlock language="python">{EXAMPLE}</CodeBlock>
          </div>
        </div>
      </div>
    </section>
  );
}

type League = {prefix: string; label: string; group?: string};

function Chips({leagues}: {leagues: League[]}): ReactNode {
  return (
    <ul className={styles.leagueList}>
      {leagues.map((l) => (
        <li key={l.prefix}>
          <Link className={styles.league} to={`/docs/${l.prefix}/`}>
            {l.label}
          </Link>
        </li>
      ))}
    </ul>
  );
}

// Every league the codegen documents (docs/src/data/leagues.json, written by tools/codegen/generate.py):
// a sport's headline leagues first, then one labelled row per registry `group`, in registry order.
function LeagueGrid(): ReactNode {
  return (
    <section className={styles.leagues} aria-labelledby="leagues">
      <div className="container">
        <h2 id="leagues">Leagues</h2>
        {registry.sports.map((sport) => {
          const leagues = sport.leagues as League[];
          const groups = [...new Set(leagues.flatMap((l) => (l.group ? [l.group] : [])))];
          const headline = leagues.filter((l) => !l.group);
          return (
            <div key={sport.key} className={styles.sport}>
              <h3 className={styles.sportTitle}>
                <Link to={`/docs/leagues/${sport.key}`}>{sport.label}</Link>
              </h3>
              {headline.length > 0 && <Chips leagues={headline} />}
              {groups.map((g) => (
                <div key={g} className={styles.group}>
                  <h4 className={styles.groupTitle}>{g}</h4>
                  <Chips leagues={leagues.filter((l) => l.group === g)} />
                </div>
              ))}
            </div>
          );
        })}
      </div>
    </section>
  );
}

function Providers(): ReactNode {
  return (
    <section className={styles.providers} aria-labelledby="providers">
      <div className="container">
        <h2 id="providers">Data sources</h2>
        <p>
          {FEATURED.map((p, i) => (
            <span key={p.key}>
              {i > 0 && ' · '}
              <Link to={p.href}>{p.label}</Link>
            </span>
          ))}
          {' · '}
          <Link to="/docs/intro#supported-leagues-and-data-sources">
            +{providerRegistry.total - FEATURED.length} more
          </Link>
        </p>
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
  return (
    <Layout
      title="Sports data for Python"
      description="sportsdataverse-py: tidy play-by-play, box scores, schedules and rosters for NBA, NFL, MLB, NHL, college and 100+ more leagues, as polars or pandas DataFrames.">
      <HomepageHeader />
      <main>
        <QuickStart />
        <LeagueGrid />
        <Providers />
      </main>
    </Layout>
  );
}
