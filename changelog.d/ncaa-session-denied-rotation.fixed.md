- **stats.ncaa.org:** an Akamai HTTP 403 "Access Denied" in the browser transport now drops the session and rotates at
  once. It used to read as "bm-verify not passed" and spend three re-solve navigations on a refused session. (#PR)
