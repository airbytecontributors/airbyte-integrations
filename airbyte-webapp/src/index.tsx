import React, { lazy, Suspense } from "react";
import ReactDOM from "react-dom";

const CloudApp = lazy(() => import(`packages/cloud/App`));
const App = lazy(() => import(`./App`));

ReactDOM.render(
  <Suspense fallback={null}>
    {process.env.REACT_APP_CLOUD ? <App /> : <CloudApp />}
  </Suspense>,
  document.getElementById("root")
);
