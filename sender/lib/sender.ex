defmodule Sender do
  @moduledoc """
  Documentation for `Sender`.
  """

  def send_email("konnichiwa@world.com" = _email),
    do: :error

  def send_email(email) do
    Process.sleep(3000)
    IO.puts("Email to #{email} sent")
    {:ok, "email_sent"}
  end

  def notify_all(emails) do
    emails
    |> Enum.map(fn email ->
      Task.async(fn ->
        send_email(email)
      end)
    end)
    |> Enum.map(&Task.await/1)
  end

  def stream_notify_all(emails) do
    emails
    |> Task.async_stream(&send_email/1, on_timeout: :kill_task)
    |> Enum.to_list()
  end

  def supervised_notify_all(emails) do
    Sender.EmailTaskSupervisor
    |> Task.Supervisor.async_stream_nolink(emails, &send_email/1)
    |> Enum.to_list()
  end
end
