import type {ReactNode} from 'react';
import clsx from 'clsx';
import Layout from '@theme/Layout';
import Link from '@docusaurus/Link';
import useDocusaurusContext from '@docusaurus/useDocusaurusContext';
import useBaseUrl from '@docusaurus/useBaseUrl';
import styles from './styles.module.css';

type FeatureItem = {
  title: string;
  imageUrl?: string;
  description: ReactNode;
};

const FeatureList: FeatureItem[] = [
  {
    title: 'Basketball',
    description: (
      <>
        Tidy NBA, WNBA, and NCAA men's & women's basketball — play-by-play, box
        scores, schedules, rosters, and standings — via the cross-league{' '}
        <code>espn_nba_*</code> / <code>espn_wnba_*</code> / <code>espn_mbb_*</code>{' '}
        / <code>espn_wbb_*</code> wrappers, mirroring{' '}
        <Link to="https://hoopR.sportsdataverse.org">hoopR</Link> and{' '}
        <Link to="https://wehoop.sportsdataverse.org">wehoop</Link>.
      </>
    ),
  },
  {
    title: 'Football',
    description: (
      <>
        College football and the NFL: ESPN play-by-play, schedules, teams, and QBR
        through <code>espn_cfb_*</code> / <code>espn_nfl_*</code>, plus an{' '}
        <code>nfl</code> module that mirrors{' '}
        <Link to="https://github.com/nflverse/nflreadpy">nflreadpy</Link> and reads{' '}
        <Link to="https://nflverse.nflverse.com">nflverse</Link> releases. Aligned
        with <Link to="https://cfbfastR.sportsdataverse.org">cfbfastR</Link>.
      </>
    ),
  },
  {
    title: 'Baseball',
    description: (
      <>
        MLB across three surfaces — ESPN, the official MLB Stats API
        (<code>mlb_api_*</code>), and Baseball Savant / Statcast — for schedules,
        play-by-play, rosters, and pitch-level data. The Python companion to{' '}
        <Link to="https://billpetti.github.io/baseballr/">baseballr</Link>.
      </>
    ),
  },
  {
    title: 'Hockey',
    description: (
      <>
        NHL & PWHL via ESPN plus the NHL's own modern APIs: the{' '}
        <code>api-web.nhle.com</code> game feed (<code>nhl_*</code>), EDGE player
        tracking (<code>nhl_edge_*</code>), Stats REST, and the Records site —
        mirroring{' '}
        <Link to="https://fastRhockey.sportsdataverse.org">fastRhockey</Link>.
      </>
    ),
  },
  {
    title: 'Tidy by default',
    description: (
      <>
        Every wrapper returns raw JSON by default; opt into an analysis-ready{' '}
        <strong>polars</strong> (or pandas) DataFrame with{' '}
        <code>return_parsed=True</code>, a <code>parse_*</code> function, or the{' '}
        <code>sportsdataverse.parsed.*</code> mirror. Whole seasons load from
        pre-built parquet via <code>load_*</code>.
      </>
    ),
  },
  {
    title: 'Part of the SportsDataverse',
    description: (
      <>
        Free and open, with one mental model across sports <em>and</em> languages —
        the function you know in R is the call you make in Python — plus
        benchmarkable EP/WP models. See{' '}
        <Link to="/docs/ecosystem">Ecosystem &amp; philosophy</Link> for the full
        Python ↔ R map.
      </>
    ),
  },
];

function Feature({imageUrl, title, description}: FeatureItem): ReactNode {
  const imgUrl = useBaseUrl(imageUrl);
  return (
    <div className={clsx('col col--4', styles.feature)}>
      {imgUrl && (
        <div className="text--center">
          <img className={styles.featureImage} src={imgUrl} alt={title} />
        </div>
      )}
      <h3>{title}</h3>
      <p>{description}</p>
    </div>
  );
}

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

export default function Home(): ReactNode {
  const {siteConfig} = useDocusaurusContext();
  return (
    <Layout
      title={siteConfig.title}
      description="The SportsDataverse's Python Package for Sports Data.">
      <HomepageHeader />
      <main>
        <section className={styles.features}>
          <div className="container">
            <div className="row">
              {FeatureList.map((props, idx) => (
                <Feature key={idx} {...props} />
              ))}
            </div>
          </div>
        </section>
      </main>
    </Layout>
  );
}
