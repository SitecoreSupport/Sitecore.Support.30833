namespace Sitecore.Support.Publishing.Service.Events
{
    using System;
    using System.Collections.Generic;
    using System.Linq;
    using Sitecore.Data;
    using Sitecore.Data.Managers;
    using Sitecore.Framework.Publishing.Item;
    using Sitecore.Framework.Publishing.Manifest;
    using Sitecore.Publishing.Service.EventQueue;
    using Sitecore.Publishing.Service.SitecoreAbstractions;
    using ScLanguage = Sitecore.Globalization.Language;
    using ScVersion = Sitecore.Data.Version;
    using Sitecore.Publishing.Service;
    using System.Collections.Concurrent;

    public class RemoteItemEventFactory : Sitecore.Publishing.Service.Events.RemoteItemEventFactory
    {
        protected readonly ConcurrentDictionary<string, ScLanguage> _threadSafeLangCache = new ConcurrentDictionary<string, ScLanguage>();

        public new IEnumerable<QueuedEventDescriptor> ConvertToRemoteItemEvents(
            ManifestOperationResult<ItemResult> operationResult,
            IDatabase targetDatabase,
            ILanguage defaultLanguage)
        {
            var itemId = new ID(operationResult.EntityId);

            Framework.Publishing.Manifest.ItemProperties propertyChanges = null;

            if (operationResult.Type == ManifestOperationResultType.Created ||
                operationResult.Type == ManifestOperationResultType.Modified)
            {
                propertyChanges = operationResult.Metadata.PropertyChanges.Current;
            }
            else
            {
                propertyChanges = operationResult.Metadata.PropertyChanges.Previous;
            }

            var itemName = propertyChanges.Name;
            var itemTemplate = new ID(propertyChanges.TemplateId);

            var itemBranch = ID.Null;

            if (propertyChanges.MasterId != null)
            {
                itemBranch = ID.Parse(propertyChanges.MasterId);
            }

            var firstVarianceChange = operationResult.Metadata.VarianceChanges.FirstOrDefault();
            var language = defaultLanguage.Value;
            ScVersion version = ScVersion.Latest;

            // this should pretty much always be not null
            if (firstVarianceChange != null)
            {
                language = LoadLanguage(firstVarianceChange.Item1, defaultLanguage.Value);
                version = ScVersion.Parse(firstVarianceChange.Item2);
            }

            var item = TemporaryItemFactory.ConstructTemporaryItem(
                itemId,
                itemName,
                itemTemplate,
                itemBranch,
                language,
                version,
                targetDatabase.Database,
                new FieldList());

            if (operationResult.Type == ManifestOperationResultType.Created)
            {
                var createdResult = operationResult.Metadata.AsCreated();
                yield return Describe(CreateCreatedEvent(createdResult, item));

                var modifiedResult = ItemResult.Modified(
                        createdResult.CreatedProperties,
                        createdResult.CreatedProperties,
                        createdResult.CreatedVariances
                            .Select(v => new
                            {
                                variance = (IVarianceIdentifier)new VarianceIdentifier(v.Item1, v.Item2),
                                change = v.Item3
                            })
                            .ToDictionary(x => x.variance, x => x.change),
                        Enumerable.Empty<FieldResult>()).AsModified();

                yield return Describe(CreateSavedEvent(modifiedResult, item));
            }

            else if (operationResult.Type == ManifestOperationResultType.Modified)
            {
                if (operationResult.Metadata.PropertyChanges.IsMoved())
                {
                    yield return Describe(CreateMovedEvent(operationResult.Metadata.AsModified(), item));
                }

                yield return Describe(CreateSavedEvent(operationResult.Metadata.AsModified(), item));
            }

            else if (operationResult.Type == ManifestOperationResultType.Deleted)
            {
                yield return Describe(CreateDeletedEvent(operationResult.Metadata.AsDeleted(), item));
            }

            else
            {
                throw new ArgumentOutOfRangeException(string.Format("The ManifestOperationResultType of {0} was not recognised.", operationResult.Type.DisplayName));
            }
        }

        protected new ScLanguage LoadLanguage(string languageName, ScLanguage defaultLang)
        {
            if (string.IsNullOrEmpty(languageName)) return defaultLang;

            ScLanguage result;
            if (!_threadSafeLangCache.TryGetValue(languageName, out result))
            {
                result = LanguageManager.GetLanguage(languageName);

                _threadSafeLangCache.TryAdd(languageName, result);
            }

            return result;
        }
    }
}