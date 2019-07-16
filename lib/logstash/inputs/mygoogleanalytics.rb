# encoding: utf-8
require "logstash/inputs/base"
require "stud/interval"
#require "socket" # for Socket.gethostname

# Generate a repeating message.
#
# This plugin is intented only as an example.

class LogStash::Inputs::Mygoogleanalytics < LogStash::Inputs::Base
  config_name "mygoogleanalytics"

  # If undefined, Logstash will complain, even if codec is unused.
  default :codec, "plain"

  # The default, `1`, means send a message every second.
  config :interval, :validate => :number, :require => false
  
  #file credential json generate by google analytics reporting api
  config :json_key_file, :validate => :string ,:require => true

  config :start_date, :validate => :string, :default => "yesterday"

  #yesterday, today, NdaysAgo or YYYY-MM-DD
  config :end_date, :validate => :string, :default => "today"

  config :dimensions, :validate => :array, :default => nil

  config :metrics, :validate => :array, :require => true

  config :view_id, :validate => :string, :require => true

  config :schedule, :validate => :string

  public
  def register
    require "google/apis/analyticsreporting_v4"
    require "rufus/scheduler"
  end # def register

  def run(queue)
    # we can abort the loop if stop? becomes true
    #while !stop?
      if @schedule
        @scheduler = Rufus::Scheduler.new(:max_work_threads => 1)
        @scheduler.cron @schedule do
          execute_query(queue)
        end

        @scheduler.join
      else
        execute_query(queue)
      end

      # attributes = {}
      # event = LogStash::Event.new(attributes)
      # decorate(event)
      # queue << event
      # because the sleep interval can be big, when shutdown happens
      # we want to be able to abort the sleep
      # Stud.stoppable_sleep will frequently evaluate the given block
      # and abort the sleep(@interval) if the return value is true
      #Stud.stoppable_sleep(@interval) { stop? }
    #end # loop
  end # def run

  def stop
    # nothing to do in this case so it is not necessary to define stop
    # examples of common "stop" tasks:
    #  * close sockets (unblocking blocking reads/accepts)
    #  * cleanup temporary files
    #  * terminate spawned threads
    @scheduler.shutdown(:wait) if @scheduler
  end

  private

  def execute_query(queue)
    google_client = get_service()
    report_request = config_options
    response = get_response(google_client, report_request)

    response.reports.each do |report|
      if report.data.rows.first
        header = []
        # @dimensions.each { |x| header<<x}
        # @metrics.each {|y| header<<y}

        unless report.column_header.dimensions.nil?
          report.column_header.dimensions.each do |dms|
            header << dms.to_s
          end
      end
        report.column_header.metric_header.metric_header_entries.each do |mtr|
          header << mtr.name.to_s
        end

        report.data.rows.each do |row|
          event = LogStash::Event.new()
          # decorate(event)
          a = []
          unless row.dimensions.nil?
            row.dimensions.each do |dimension|
              a << dimension
            end
        end

          row.metrics.each do |metric|
            metric.values.each do |value|
              a << value
            end
          end
          
          header.zip(a).each do |head, data|
            event.set(head, data)
          end
          decorate(event)
          queue << event
        end #end do each row
      end #end if
    end #end do each
  end

  def get_service
    service = Google::Apis::AnalyticsreportingV4::AnalyticsReportingService.new

    credentials = Google::Auth::ServiceAccountCredentials.make_creds(
      json_key_io: File.open(@json_key_file),
      scope: 'https://www.googleapis.com/auth/analytics.readonly'
    )

    # Authorize with our readonly credentials
    service.authorization = credentials

    google_client = service
    return google_client
  end

  def config_options
    dms_arr = []
    order_bys = []

    if @dimensions
      @dimensions.each do |name_dms|
        dms = Google::Apis::AnalyticsreportingV4::Dimension.new(
          name: name_dms
        )
        ob = Google::Apis::AnalyticsreportingV4::OrderBy.new(
          field_name: name_dms
        )
        dms_arr << dms
        order_bys << ob
      end
    end

    mtr_arr = []
    if @metrics
      @metrics.each do |expression|
        metric = Google::Apis::AnalyticsreportingV4::Metric.new(
          expression: expression
        )
        mtr_arr << metric
      end
    end

    date_range = Google::Apis::AnalyticsreportingV4::DateRange.new(
      start_date: @start_date,
      end_date: @end_date
    )

    report_request = Google::Apis::AnalyticsreportingV4::ReportRequest.new(
      view_id: @view_id,
      date_ranges: [date_range],
      metrics: mtr_arr,
      dimensions: dms_arr,
      order_bys: order_bys,
      include_empty_rows: true
    )
    return report_request
  end

  def get_response(google_client, report_request)
    # Create a new report request
    request = Google::Apis::AnalyticsreportingV4::GetReportsRequest.new(
      { report_requests: [report_request] }
    )
    # Make API call.
    response = google_client.batch_get_reports(request)
    return response
  end

end # class LogStash::Inputs::Mygoogleanalytics
