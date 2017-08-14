/**
 * Background Cloud Function to be triggered by Cloud Storage.
 *
 * @param {object} event The Cloud Functions event.
 * @param {function} callback The callback function.
 */
const google = require("googleapis");
const dataflow = google.dataflow("v1b3");

const config: Config = require('./config.json');

interface Config {
  PROJECT_ID: string,
  DATAFLOW_TEMPLATE_PATH: string
}

exports.dataflowJob = (event: any, callback: Function) => {
  let date = new Date();
  let time = date.getTime();

  const file = event.data;
  if (file.resourceState === 'not_exists') {
    console.log(`File ${file.name} deleted.`);
    return;
  } else if (file.metageneration === '1') {
    // metageneration attribute is updated on metadata changes.
    // on create value is 1
    console.log(`File ${file.name} uploaded.`);
  } else {
    console.log(`File ${file.name} metadata updated.`);
  }

  google.auth.getApplicationDefault(function (err: any, authClient: any, projectId: string) {
    if (err) {
      throw err;
    }
    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
      authClient = authClient.createScoped([
        'https://www.googleapis.com/auth/cloud-platform',
        'https://www.googleapis.com/auth/userinfo.email'
      ]);
    }
    const dataflow = google.dataflow({
      version: 'v1b3',
      auth: authClient
    });

    console.log(`gs://${file.bucket}/${file.name}`)

    dataflow.projects.templates.launch({
      projectId: config.PROJECT_ID,
      gcsPath: config.DATAFLOW_TEMPLATE_PATH,
      resource: {
        parameters: {
          inputFile: `gs://${file.bucket}/${file.name}`
        },
        jobName: `cloud-fn-dataflow-test-${time}`
      }
    }, function (err: any, response: any) {
      if (err) {
        console.error("problem running dataflow template, error was: ", err);
      }
      console.log("Dataflow template response: ", response);
    });
  });
  callback();
};

