using Microsoft.Azure.EventGrid;
using Microsoft.Azure.EventGrid.Models;
using Microsoft.Azure.WebJobs;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Npgsql;
using System;
using System.Collections.Generic;
using System.Data;
using System.Diagnostics;

namespace PostgreSQLEventProcessing
{
    public class EventProcessing
    {
        ILogger log;
        [FunctionName("FuncEventProcessingtion")]
        public void Run([TimerTrigger("0 */5 * * * *")] TimerInfo myTimer)
        {
            log.LogInformation($"C# Timer trigger function executed at: {DateTime.Now}");
            startListening();
            
        }

        private void startListening()
        { 
          string connectionstring = string.Empty;
            try
            { 
                connectionstring = this.GetConnectionString(true);
                this.notificationConnection = new NpgsqlConnection(connectionstring);
                this.notificationConnection.Open();

                if (this.notificationConnection.State == ConnectionState.Open)
                {
                    using (var command = new NpgsqlCommand("listen " + TriggerChannelName, this.notificationConnection))
                    {
                        command.ExecuteNonQuery();
                    }

                    this.notificationConnection.Notification += this.PostgresNotification;
                }
                else
                {
                   log.LogInformation("Connection failed !");
                }
            }
            catch
            {
               log.LogInformation(" Inside the Catch block Connection error: !" + connectionstring);
            }

        }

        #region PGSQL Access

        private const string Server = "";
        private const string User = "";
        private const string Password = "";
        private const string DbName = "";
        private const string TableName = "Products";
        private NpgsqlConnection notificationConnection;
        private const string TriggerChannelName = "productsnotification";

        /// <summary>
        /// Get Npgsql connection string.
        /// </summary>
        /// <param name="syncNotification">True to sync notifications.</param>
        /// <returns>The connection string.</returns>
        private string GetConnectionString(bool syncNotification)
        {
            var csb = new NpgsqlConnectionStringBuilder
            {
                Host = Server,
                Database = DbName,
                Username = User,
                Password = Password,
                KeepAlive = 1
            };

            Debug.Print(csb.ConnectionString);
            return csb.ConnectionString;
        }

        /// <summary>
        /// Postgres notification event.
        /// </summary>
        /// <param name="sender">The sender.</param>
        /// <param name="e">The event arguments.</param>
        private void PostgresNotification(object sender, NpgsqlNotificationEventArgs e)
        {
            string info = e.AdditionalInformation;
            Console.WriteLine(@"Notification -->");
            Console.WriteLine(@"  DATA {0}", info);
            Console.WriteLine(@"  CHANNEL {0}", e.Condition);
            Console.WriteLine(@"  PID {0}", e.PID);
          
            if (PublishEvent(info))
            {
                log.LogInformation($"Information in the postgre Notification={info}");
            }
        }

        private void StopListening(string channelName)
        {
            if (this.notificationConnection != null && this.notificationConnection.State != ConnectionState.Closed)
            {
                this.notificationConnection.Notification -= this.PostgresNotification;

                using (var command = new NpgsqlCommand("unlisten " + channelName, this.notificationConnection))
                {
                    command.ExecuteNonQuery();
                }

                this.notificationConnection.Close();
            }

        }

        #endregion PGSQL access

        #region Event Grid Access

        private const string topicEndpoint = "";
        private const string topicKey = "";

        private bool PublishEvent(string oneEvent)
        {
            try
            {
                string topicHostname = new Uri(topicEndpoint).Host;
                TopicCredentials topicCredentials = new TopicCredentials(topicKey);
                EventGridClient client = new EventGridClient(topicCredentials);

                client.PublishEventsAsync(topicHostname, GetEventsList(oneEvent)).GetAwaiter().GetResult();
                return true;
            }
            catch
            {
                return false;
            }
        }

        private IList<EventGridEvent> GetEventsList(string oneEvent)
        {
            List<EventGridEvent> eventsList = new List<EventGridEvent>();

            List<string> eventDetails = new List<string>(oneEvent.Split(';'));
            eventsList.Add(new EventGridEvent()
            {
                Id = Guid.NewGuid().ToString(),
                EventType = "EComPOC.Items.ItemReceived",
                Data = new ItemReceivedEventData()
                {
                    OperationType = eventDetails[0],
                    ItemId = eventDetails[1]
                },
                EventTime = DateTime.Now,
                Subject = "ProductDataUpdate",
                DataVersion = "1.0"
            });

            return eventsList;
        }

        #endregion Event Grid Access
    }

    class ItemReceivedEventData
    {
        [JsonProperty(PropertyName = "itemId")]
        public string ItemId { get; set; }

        [JsonProperty(PropertyName = "operationType")]
        public string OperationType { get; set; }

    }
}

