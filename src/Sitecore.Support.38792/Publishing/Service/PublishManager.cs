using System;
using System.Collections.Generic;
using System.Globalization;
using System.Linq;
using System.Threading;
using Sitecore.Abstractions;
using Sitecore.Collections;
using Sitecore.Configuration;
using Sitecore.Data;
using Sitecore.Data.Archiving;
using Sitecore.Data.Engines.DataCommands;
using Sitecore.Data.Events;
using Sitecore.Data.Items;
using Sitecore.Diagnostics;
using Sitecore.Eventing;
using Sitecore.Framework.Publishing;
using Sitecore.Framework.Publishing.Locators;
using Sitecore.Framework.Publishing.PublisherOperations;
using Sitecore.Framework.Publishing.PublishJobQueue;
using Sitecore.Globalization;
using Sitecore.Jobs;
using Sitecore.Publishing;
using Sitecore.Publishing.Diagnostics;
using Sitecore.Publishing.Pipelines.Publish;
using Sitecore.Publishing.Recovery;
using Sitecore.Publishing.Service;
using Sitecore.Publishing.Service.ItemEvents;
using Sitecore.Publishing.Service.JobQueue;
using Sitecore.Publishing.Service.SitecoreAbstractions;

namespace Sitecore.Support.Publishing.Service
{
  public class PublishManager : DefaultPublishManager
  {
    private readonly IPublishingJobProvider _jobProvider;
    private readonly IPublishJobQueueService _publishJobQueueService;
    private readonly IPublisherOperationService _publisherOpsService;
    private readonly IItemOperationEmitter _operationEmitter;
    private readonly IPublishRecoveryStrategy _recoveryStrategy = new NullPublishingServiceRecoverStrategy();
    private readonly ISitecoreSettings _sitecoreSettings;
    private readonly IDatabaseFactory _databaseFactory;
    private readonly IEventManager _eventManager;

    public PublishManager(
        IPublishingJobProviderFactory providerFactory,
        IPublishJobQueueService publishJobQueueService,
        IPublisherOperationService publisherOpsService,
        IItemOperationEmitter operationEmitter,
        BaseLanguageManager languageManager,
        BaseFactory factory,
        BaseLog log,
        ProviderHelper<PublishProvider, PublishProviderCollection> providerHelper)
        : this(providerFactory,
              publishJobQueueService,
              publisherOpsService,
              operationEmitter,
              new EventManagerWrapper(),
              new SitecoreSettingsWrapper(),
              new DatabaseFactoryWrapper(new PublishingLogWrapper()),
              languageManager,
              factory,
              log,
              providerHelper,
              new DefaultEventQueueProvider())
    {
    }

    public PublishManager(
        IPublishingJobProviderFactory providerFactory,
        IPublishJobQueueService publishJobQueueService,
        IPublisherOperationService publisherOpsService,
        IItemOperationEmitter operationEmitter,
        IEventManager eventManager,
        ISitecoreSettings sitecoreSettings,
        IDatabaseFactory databaseFactory,
        BaseLanguageManager languageManager,
        BaseFactory factory,
        BaseLog log,
        ProviderHelper<PublishProvider, PublishProviderCollection> providerHelper,
        BaseEventQueueProvider eventQueueProvider)
        : base
        (languageManager, factory, log, providerHelper, eventQueueProvider)
    {
      _publishJobQueueService = publishJobQueueService;
      _publisherOpsService = publisherOpsService;
      _operationEmitter = operationEmitter;
      _eventManager = eventManager;
      _sitecoreSettings = sitecoreSettings;
      _jobProvider = providerFactory.GetPublishingJobProvider();
      _databaseFactory = databaseFactory;
    }

    public override IPublishRecoveryStrategy PublishRecoveryStrategy
    {
      get { return _recoveryStrategy; }

      protected set
      {
        // always return null strategy
      }
    }

    public override ItemList GetPublishingTargets(Database database)
    {
      var publishingTargets = new ItemList();

      if (database == null)
      {
        return publishingTargets;
      }

      var publishingTargetRoot = database.GetItem("/sitecore/system/publishing targets");

      return publishingTargetRoot == null ? publishingTargets : publishingTargetRoot.GetChildren(ChildListOptions.SkipSorting).InnerChildren;
    }

    public override PublishStatus GetStatus(Handle handle)
    {
      Error.AssertObject(handle, "handle");

      var job = JobManager.GetJob(handle);

      if (job != null)
      {
        return job.Options.CustomData as PublishStatus;
      }

      return null;
    }

    public override void Initialize()
    {
      if (!Settings.Publishing.Enabled)
      {
        PublishingLog.Warn("Publishing is disabled due to running under a restricted license.");
        return;
      }

      _eventManager.Subscribe<RestoreItemCompletedEvent>((e, c) => RestoreItemCompletedEventHandler(e, c));

      // setup the data engine event handlers...
      foreach (var engine in Factory.GetDatabases()
          .Where(db => db.Name == _sitecoreSettings.DefaultSourceDatabaseName)
          .Select(db => db.Engines.DataEngine))
      {
        engine.AddedVersion += DataEngine_AddedVersion;
        engine.CopiedItem += DataEngine_CopiedItem;
        engine.CreatedItem += DataEngine_CreatedItem;
        engine.DeletedItem += DataEngine_DeletedItem;
        engine.MovedItem += DataEngine_MovedItem;
        engine.RemovedVersion += DataEngine_RemovedVersion;
        engine.SavedItem += DataEngine_SavedItem;
      }
    }

    public override Handle PublishIncremental(Database source, Database[] targets, Language[] languages, Language clientLanguage)
    {
      return EnqueuePublishJob(
          "PublishIncremental",
          targets,
          languages,
          Context.User.Name,
          Context.Language.Name,
          true,
          false,
          true,
          PublishJobType.Incremental);
    }

    public override Handle PublishItem(Item item, Database[] targets, Language[] languages, bool deep, bool compareRevisions, bool publishRelatedItems)
    {
      return EnqueuePublishJob(
          "PublishItem",
          targets,
          languages,
          Context.User.Name,
          Context.Language.Name,
          deep,
          publishRelatedItems,
          true,
          PublishJobType.SingleItem,
          item);
    }

    public override Handle PublishSmart(Database source, Database[] targets, Language[] languages, Language clientLanguage)
    {
      return Republish(source, targets, languages, clientLanguage);
    }

    public override Handle Republish(Database source, Database[] targets, Language[] languages, Language clientLanguage)
    {
      return EnqueuePublishJob(
          "PublishSite",
          targets,
          languages,
          Context.User.Name,
          Context.Language.Name,
          true,
          false,
          false,
          PublishJobType.Full,
          source.GetRootItem());
    }

    public override Handle Publish(Sitecore.Publishing.PublishOptions[] options, Language clientLanguage)
    {
      var builtOptions = options.Select(x =>
      {
        var rootId = x.RootItem != null ? x.RootItem.ID.Guid : new Guid?();
        var serviceOptions = new Framework.Publishing.PublishJobQueue.PublishOptions(
            x.Deep,
            x.PublishRelatedItems,
            Context.User.Name,
            Context.Language.Name,
            new[]
            {
                        x.Language.Name
            },
            PopulateTargets(GetPublishingTargets(x.SourceDatabase), new[]
            {
                        x.TargetDatabase
            }),
            rootId,
            null,
            x.SourceDatabase.Name);

        var publishType = rootId.HasValue ? PublishJobType.SingleItem : PublishJobType.Incremental;
        serviceOptions.SetPublishType(publishType.ToString());
        serviceOptions.SetDetectCloneSources(true);

        return serviceOptions;
      }).ToArray();

      var publishStatus = new PublishStatus();
      publishStatus.SetCurrentLanguages(options.Select(x => x.Language).Distinct());
      publishStatus.SetCurrentTarget(options.Select(x => x.TargetDatabase).Distinct().First());

      var jobOptions = BuildPublishJobOptions(
          "Publish",
          builtOptions,
          publishStatus);

      var job = JobManager.Start(jobOptions);

      return job.Handle;
    }

    public override IdList GetPublishQueue(DateTime from, DateTime to, Database database)
    {
      //TODO: To be implemented when service provides API
      PublishingLog.Audit("'GetPublishQueue' is not implemented in framework publishing.");
      return base.GetPublishQueue(from, to, database);
    }

    public override List<PublishQueueEntry> GetPublishQueueEntries(DateTime from, DateTime to, Database database)
    {
      //TODO: To be implemented when service provides API
      PublishingLog.Audit("'GetPublishQueueEntries' is not implemented in framework publishing.");
      return base.GetPublishQueueEntries(from, to, database);
    }

    public override PublishQueue GetPublishQueue(Database database)
    {
      var message = "'GetPublishQueue (PublishQueue)' is not implemented in framework publishing.";
      PublishingLog.Audit(message);
      throw new NotSupportedException(message);
    }

    public override bool IsSmartPublishScheduled(Database database, string targetDatabaseName, Language language)
    {
      var message = "'IsSmartPublishScheduled' is not implemented in framework publishing.";
      PublishingLog.Audit(message);
      throw new NotSupportedException(message);
    }

    public override Handle RebuildDatabase(Database source, Database[] targets)
    {
      var message = "'RebuildDatabase' is not implemented in framework publishing.";
      PublishingLog.Audit(message);
      throw new NotSupportedException(message);
    }

    protected virtual Handle EnqueuePublishJob(
        string jobName,
        Database[] targets,
        Language[] languages,
        string username,
        string contextLanguage,
        bool publishDescendants,
        bool publishRelated,
        bool detectCloneSources,
        PublishJobType publishType,
        Item startItem = null)
    {
      var publishStatus = new PublishStatus();
      publishStatus.SetCurrentLanguages(languages);
      publishStatus.SetCurrentTarget(targets.First());

      var db = _databaseFactory.GetDatabase(_sitecoreSettings.DefaultSourceDatabaseName).Database; //Need to get a default DB is there is no item.
      Guid? itemId = null;

      if (startItem != null)
      {
        db = startItem.Database;
        itemId = startItem.ID.Guid;
      }

      var options = new Framework.Publishing.PublishJobQueue.PublishOptions(
          publishDescendants,
          publishRelated,
          username,
          contextLanguage,
          languages.Select(l => l.Name),
          PopulateTargets(GetPublishingTargets(db), targets),
          itemId,
          null,
          db.Name);

      options.SetPublishType(publishType.ToString());
      options.SetDetectCloneSources(detectCloneSources);

      var jobOptions = BuildPublishJobOptions(
          jobName,
          new[]
          {
                    options
          },
          publishStatus);
      jobOptions.AtomicExecution = false;

      var job = JobManager.Start(jobOptions);

      return job.Handle;
    }

    private void ExecutePublishAction(Framework.Publishing.PublishJobQueue.PublishOptions[] options, PublishStatus publishStatus)
    {
      publishStatus.SetState(JobState.Running);

      try
      {
        foreach (var option in options)
        {
          var publishJob = _jobProvider.Add(option).Result;
          var jobId = publishJob.Id;

          publishStatus.SetProcessed(1);
          publishStatus.Messages.Add("Publish Requested");
          publishStatus.Messages.Add(string.Format(CultureInfo.InvariantCulture, "\nQueued on : {0}", publishJob.Queued));

          if (publishJob == null)
          {
            throw new ApplicationException(string.Format(CultureInfo.InvariantCulture, "Unable to retrieve job. Id: {0}", jobId));
          }

          publishStatus.Messages.Add("\nJob Id : " + publishJob.Id + "\n");
          publishStatus.Messages.Add(string.Format(CultureInfo.InvariantCulture, "Status: {0} ({1})", publishJob.StatusMessage, publishJob.Status.ToString()));

          var status = publishJob.Status;

          while (status == PublishJobStatus.Queued || status == PublishJobStatus.Started)
          {
            if (status == PublishJobStatus.Started)
            {
              publishStatus.SetState(JobState.Running);
            }

            Thread.Sleep(TimeSpan.FromSeconds(3));
            publishJob = _publishJobQueueService.GetJob(jobId).Result;
            if (publishJob == null)
            {
              throw new ApplicationException(string.Format(CultureInfo.InvariantCulture, "Unable to retreive job. Id: {0}", jobId));
            }
            status = publishJob.Status;
          }

          publishStatus.Messages.Add(string.Format(CultureInfo.InvariantCulture, "Job Finished: {0} ({1})", publishJob.StatusMessage, publishJob.Status.ToString()));
          publishStatus.Messages.Add(string.Format(CultureInfo.InvariantCulture, "Job Completed on: {0} ", publishJob.Stopped));
          publishStatus.SetFailed(false);
          publishStatus.SetState(JobState.Finished);
        }
      }
      catch (Exception ex)
      {
        publishStatus.Messages.Add(string.Format(CultureInfo.InvariantCulture, "A serious error occured executing the publish job. {0} ({1})", ex.ToString(), ex.Message));
        publishStatus.SetFailed(true);
        publishStatus.SetState(JobState.Finished);
      }
    }

    private JobOptions BuildPublishJobOptions(string jobName, Framework.Publishing.PublishJobQueue.PublishOptions[] options, PublishStatus publishStatus)
    {
      publishStatus.SetState(JobState.Queued);
      var clientLanguage = Language.Parse(Settings.DefaultLanguage);

      var jobOptions = new JobOptions(
          jobName,
          "PublishProvider",
          Constants.PublishingSiteName,
          this,
          "ExecutePublishAction",
          new object[]
          {
                    options,
                    publishStatus
          })
      {
        ContextUser = Context.User,
        ClientLanguage = clientLanguage,
        AfterLife = TimeSpan.FromMilliseconds(Settings.Publishing.PublishDialogPollingInterval * 2),
        Priority = Settings.Publishing.ThreadPriority,
        ExecuteInManagedThreadPool = Settings.Publishing.ExecuteInManagedThreadPool,
        CustomData = publishStatus
      };

      return jobOptions;
    }

    // DataEngine Event handlers ...

    /// <summary>
    /// Handles the AddedVersion event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_AddedVersion([CanBeNull] object sender, [NotNull] ExecutedEventArgs<AddVersionCommand> e)
    {
      EmitItemEvent(e.Command.Result, DateTime.UtcNow, GetRestrictions(e.Command.Result), PublisherOperationType.VariantCreated);
    }

    /// <summary>
    /// Handles the CopiedItem event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_CopiedItem([CanBeNull] object sender, [NotNull] ExecutedEventArgs<CopyItemCommand> e)
    {
      EmitItemEvent(e.Command.Result, DateTime.UtcNow, GetRestrictions(e.Command.Result), PublisherOperationType.VariantSaved);
    }

    /// <summary>
    /// Handles the CreatedItem event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_CreatedItem([CanBeNull] object sender, [NotNull] ExecutedEventArgs<CreateItemCommand> e)
    {
      EmitItemEvent(e.Command.Result, DateTime.UtcNow, GetRestrictions(e.Command.Result), PublisherOperationType.VariantCreated);
    }

    /// <summary>
    /// Handles the DeletedItem event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_DeletedItem([CanBeNull] object sender, [NotNull] ExecutedEventArgs<DeleteItemCommand> e)
    {
      if (string.IsNullOrWhiteSpace(e.Command.Item.Language.Name))
      {
        PublishingLog.Debug(string.Format(CultureInfo.InvariantCulture, "Item: {0} has an empty language value, item deleted event will be skipped", e.Command.Item.ID));
        return;
      }

      // override the itemPath as its not being passed by the data engine in this event
      var parentPath = GetItemPath(e.Command.Database.GetItem(e.Command.ParentId));
      var fullPath = string.Format(CultureInfo.InvariantCulture, "{0}/{1}", parentPath, e.Command.Item.ID.ToString());
      EmitItemEvent(e.Command.Item, DateTime.UtcNow, GetRestrictions(e.Command.Item), PublisherOperationType.AllVariantsDeleted, fullPath);
    }

    /// <summary>
    /// Handles the MovedItem event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_MovedItem([CanBeNull] object sender, [NotNull] ExecutedEventArgs<MoveItemCommand> e)
    {
      if (string.IsNullOrWhiteSpace(e.Command.Item.Language.Name))
      {
        PublishingLog.Debug(string.Format(CultureInfo.InvariantCulture, "Item: {0} has an empty language value, item moved event will be skipped", e.Command.Item.ID));
        return;
      }

      EmitItemEvent(e.Command.Item, DateTime.UtcNow, GetRestrictions(e.Command.Item), PublisherOperationType.VariantSaved);
    }

    /// <summary>
    /// Handles the RemovedVersion event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_RemovedVersion([CanBeNull] object sender, [NotNull] ExecutedEventArgs<RemoveVersionCommand> e)
    {
      EmitItemEvent(e.Command.Item, DateTime.UtcNow, GetRestrictions(e.Command.Item), PublisherOperationType.VariantDeleted);
    }

    /// <summary>
    /// Handles the SavedItem event of the DataEngine control.
    /// </summary>
    /// <param name="sender">The source of the event.</param>
    /// <param name="e">The <see cref="ExecutedEventArgs{TCommand}"/> instance containing the event data.</param>
    private new void DataEngine_SavedItem([CanBeNull] object sender, [NotNull] ExecutedEventArgs<SaveItemCommand> e)
    {
      if (string.IsNullOrWhiteSpace(e.Command.Item.Language.Name))
      {
        PublishingLog.Debug(string.Format(CultureInfo.InvariantCulture, "Item: {0} has an empty language value, item saved event will be skipped", e.Command.Item.ID));
        return;
      }

      EmitItemEvent(e.Command.Item, DateTime.UtcNow, GetRestrictions(e.Command.Item), PublisherOperationType.VariantSaved);
    }

    private void RestoreItemCompletedEventHandler(RestoreItemCompletedEvent e, EventContext context)
    {
      var db = _databaseFactory.GetDatabase(e.DatabaseName);
      var item = db.GetItem(new ID(e.ItemId));

      EmitItemEvent(item, DateTime.UtcNow, GetRestrictions(item), PublisherOperationType.VariantCreated);
    }

    private void EmitItemEvent(Item item, DateTime timestamp, ItemOperationRestrictions restrictions, PublisherOperationType operation, string itemPath = null)
    {
      var finalRevision = TimeBasedGuidFactory.GenerateTimeBasedGuid();

      var revisionFieldValue = item.Fields[FieldIDs.Revision].Value;

      if (!string.IsNullOrEmpty(revisionFieldValue))
      {
        finalRevision = Guid.Parse(revisionFieldValue);
      }

      itemPath = string.IsNullOrWhiteSpace(itemPath)
          ? GetItemPath(item)
          : itemPath;

      _operationEmitter.PostOperation(
          new ItemOperationData(
              DateTime.UtcNow,
              restrictions,
              new ItemVariantLocator(
                  item.Database.Name,
                  item.ID.Guid,
                  item.Language.Name,
                  item.Version.Number),
              finalRevision,
              itemPath,
              operation)
      );
    }

    private ItemOperationRestrictions GetRestrictions(Item item)
    {
      return new ItemOperationRestrictions(
          item.Publishing.PublishDate,
          item.Publishing.UnpublishDate,
          item.Publishing.ValidFrom,
          item.Publishing.ValidTo);
    }

    private string GetItemPath(Item item)
    {
      return item.Paths.GetPath(ItemPathType.ItemID);
    }

    private IEnumerable<string> PopulateTargets(ItemList fullTargetList, Database[] targetDbsFromDialog)
    {
      foreach (var item in fullTargetList)
      {
        var targetDbName = item["Target database"];

        if (string.IsNullOrWhiteSpace(targetDbName))
        {
          continue;
        }

        foreach (var database in targetDbsFromDialog)
        {
          if (string.Equals(database.Name, targetDbName, StringComparison.OrdinalIgnoreCase))
          {
            yield return item.Name;
          }
        }
      }
    }
  }
}
