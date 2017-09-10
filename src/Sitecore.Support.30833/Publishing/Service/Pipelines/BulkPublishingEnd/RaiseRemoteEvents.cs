namespace Sitecore.Support.Publishing.Service.Pipelines.BulkPublishingEnd
{
    using Sitecore.Framework.Publishing.Eventing.Remote;
    using Sitecore.Framework.Publishing.Manifest;
    using Sitecore.Publishing.Service.EventQueue;
    using Sitecore.Publishing.Service.Events;
    using Sitecore.Publishing.Service.Pipelines.BulkPublishingEnd;
    using Sitecore.Publishing.Service.Security;
    using Sitecore.Publishing.Service.SitecoreAbstractions;
    using Sitecore.Support.Publishing.Service.Events;
    using System;
    using System.Linq;
    using System.Reflection;
    using System.Threading.Tasks;

    public class RaiseRemoteEvents : Sitecore.Publishing.Service.Pipelines.BulkPublishingEnd.RaiseRemoteEvents
    {
        protected readonly PatchedRemoteItemEventFactory _patchedRemoteItemEventFactory = new PatchedRemoteItemEventFactory();

        public RaiseRemoteEvents(string remoteEventCacheClearingThreshold, ITargetCacheClearHistory targetCacheClearHistory) : base(remoteEventCacheClearingThreshold, targetCacheClearHistory)
        { }

        public RaiseRemoteEvents(IDatabaseFactory sitecoreFactory, ILanguageManager langManager, ISitecoreSettings settings, IUserRoleService userRoleService, IPublishingLog logger, IRemoteItemEventFactory remoteItemEventFactory, ITargetCacheClearHistory targetCacheClearHistory, int remoteEventCacheClearingThreshold) : base(sitecoreFactory, langManager, settings, userRoleService, logger, remoteItemEventFactory, targetCacheClearHistory, remoteEventCacheClearingThreshold)
        { }

        protected override async Task RaiseRemoteEventsOnTarget(
        PublishEndResultBatchArgs publishEndResultBatchArgs,
        string languageName,
        int eventThreshold)
        {
            if (publishEndResultBatchArgs.TotalResultCount > eventThreshold || eventThreshold == 0)
            {
                await RaiseRemoteCacheClearingEventsOnTarget(publishEndResultBatchArgs.TargetInfo);
                _cacheClearHistory.Add(publishEndResultBatchArgs.TargetInfo.ManifestId, publishEndResultBatchArgs.TargetInfo.TargetId);
            }
            else
            {
                try
                {
                    RaiseRemoteItemEventsOnTarget(publishEndResultBatchArgs, languageName);
                }
                catch (Exception ex)
                {
                    this._logger.Info("[Sitecore.Support.30833]: ", ex);

                    throw;
                }
            }
        }

        private async Task RaiseRemoteCacheClearingEventsOnTarget(PublishingJobTargetMetadata targetInfo)
        {
            var baseType = this.GetType().BaseType;

            await (Task)baseType.GetMethod("RaiseRemoteCacheClearingEventsOnTarget", BindingFlags.NonPublic | BindingFlags.Instance).Invoke(this, new object[] { targetInfo });
        }

        private void RaiseRemoteItemEventsOnTarget(PublishEndResultBatchArgs publishEndResultBatchArgs, string languageName)
        {
            var db = _factory.GetDatabase(publishEndResultBatchArgs.TargetInfo.TargetDatabaseName);
            var publishLanguage = _langManager.GetLanguage(languageName);

            _logger.Info(
                string.Format("Starting to raise the remote item events on the publishing target : {0}", db.Name));

            using (var ctx = CreateQueueContext(db.ConnectionStringName))
            {
                var eventsRaised = LoadResultsIntoEventQueue(
                          publishEndResultBatchArgs.Batch,
                          CreateQueueRepository(ctx),
                          ctx,
                          db,
                          publishLanguage);

                _logger.Info(
                    string.Format("Completed raising the remote item events on the publishing target : {0}. {1} events raised.", db.Name, eventsRaised));
            }
        }

        protected new int LoadResultsIntoEventQueue(
      ManifestOperationResult<ItemResult>[] itemsOperationResults,
        IEventQueueRepository eventQueue,
        IDbTransactionalConnection queueContext,
        IDatabase database,
        ILanguage defaultLanguage)
        {
            var userName = _userRoleService.GetCurrentUsername();
            var instanceName = _settings.InstanceName;

            var remoteEvents = itemsOperationResults
                      .SelectMany(r => _patchedRemoteItemEventFactory.ConvertToRemoteItemEvents(r, database, defaultLanguage))
                      .ToArray();

            var eventCount = 0;

            try
            {
                queueContext.BeginTransaction();
                eventQueue.Enqueue(remoteEvents, userName, instanceName).Wait();
                queueContext.CommitTransaction();
                eventCount = remoteEvents.Length;
            }
            catch
            {
                queueContext.RollbackTransaction();
                throw;
            }

            return eventCount;
        }

    }
}
