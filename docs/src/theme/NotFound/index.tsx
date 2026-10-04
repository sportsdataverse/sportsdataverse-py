import React, {type ReactNode} from 'react';
import Link from '@docusaurus/Link';
import {PageMetadata} from '@docusaurus/theme-common';
import Layout from '@theme/Layout';
import Heading from '@theme/Heading';

// The site's 404 page. Ejected with `docusaurus swizzle @docusaurus/theme-classic NotFound --eject`,
// which Docusaurus 3.10 lists as safe; ejecting NotFound/Content on its own is marked unsafe, so this
// page renders its content itself instead of importing @theme/NotFound/Content.
export default function NotFound(): ReactNode {
  return (
    <>
      <PageMetadata title="Page not found" />
      <Layout>
        <main className="container margin-vert--xl">
          <div className="row">
            <div className="col col--6 col--offset-3">
              <Heading as="h1" className="sdv-404-title">Page not found</Heading>
              <p>
                Nothing lives at this address. The page may have moved when the docs were
                reorganized, or the link may have a typo.
              </p>
              <p>
                Search for a function or a topic with the search box at the top of the page, or
                start again from the docs.
              </p>
              <Link className="button button--primary" to="/docs/intro">
                Go to the docs
              </Link>
            </div>
          </div>
        </main>
      </Layout>
    </>
  );
}
