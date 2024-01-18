# Google Leads Form Notification Process

This function receives a POST call after a Google Form is filled out with sales lead information, and formats an email with that information to be sent to any number of email addresses. The POST request contains metadata about the form, as well as the data that was filled out within the form such as user contact information.

Using the `esquireforms/formRoutes` table, each `form_id` is mapped to any number of recipient email addresses. This table must be updated each time a new form is created, to map the form to the appropriate recepients who should receive an email.