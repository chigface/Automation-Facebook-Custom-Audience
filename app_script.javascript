/**
 * Add menu entries when spreadsheet opens
 */
function onOpen() {
  SpreadsheetApp.getUi()
  .createMenu('FaceBook Audience List')
  .addItem('Create Audience', 'CreateAudience') // Second argument is a function
  .addToUi();
}

/**
 * Create Audience by calling the http trigger from Google Cloud Function
 */
function CreateAudience() {
  // Call the webservice
  var cloud_function_url = "https://<region>-<project-id>.cloudfunctions.net/<function_name>";
  var response = UrlFetchApp.fetch(cloud_function_url);
  Browser.msgBox(response.getContentText());
}
