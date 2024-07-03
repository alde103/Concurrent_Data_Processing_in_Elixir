# defmodule PageConsumer do
#   use GenStage
#   require Logger

#   def start_link(_args) do
#     initial_state = []
#     GenStage.start_link(__MODULE__, initial_state)
#   end

#   # def init(initial_state) do
#   #   Logger.info("PageConsumer init")
#   #   {:consumer, initial_state, subscribe_to: [PageProducer]}
#   # end

#   def init(initial_state) do
#     Logger.info("PageConsumer init")
#     #sub_opts = [{PageProducer, min_demand: 0, max_demand: 3}]
#     sub_opts = [{PageProducer, min_demand: 0, max_demand: 3}]
#     {:consumer, initial_state, subscribe_to: sub_opts}
#   end

#   def handle_events(events, from, state) do
#     Logger.info("PageConsumer received #{inspect({events, from})}")
#     # Pretending that we're scraping web pages.
#     Enum.each(events, fn _page ->
#       Scraper.work()
#     end)

#     {:noreply, [], state}
#   end
# end

defmodule PageConsumer do
  require Logger

  def start_link(event) do
    Task.start_link(fn ->
      Logger.info("PageConsumer received #{event} PID: #{inspect(self())}")
      Scraper.work()
    end)
  end
end
