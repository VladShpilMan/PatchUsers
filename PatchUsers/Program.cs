using System.Security.Cryptography.X509Certificates;
using Raven.Client.Documents;
using Raven.Client.Documents.Session;
using Sparrow.Json.Parsing;

class Program
{
    static async Task Main(string[] args)
    {
        var dbUrls = new[] { "http://localhost:8080" };
        var dbName = "test.api.ravendb.net";
        var certPath = ""; 
        var certPassword = "";
        
        Console.WriteLine("Connecting to RavenDB...");

        using var store = new DocumentStore
        {
            Urls = dbUrls,
            Database = dbName,
            //Certificate = new X509Certificate2(certPath, certPassword)
        }.Initialize();

        Console.WriteLine("Connected successfully!\n");
        
        store.OnAfterConversionToDocument += OnAfterConversionToDocumentHandler;

        int batchSize = 100;
        Console.Write("Enter the number of documents to process in the first run (press Enter for 100): ");
        var input = Console.ReadLine();
        
        if (int.TryParse(input, out int parsedBatch) && parsedBatch > 0)
        {
            batchSize = parsedBatch;
        }

        int totalProcessed = 0;
        
        while (true)
        {
            Console.WriteLine($"\n--- Processing {batchSize} documents... ---");
            
            using var session = store.OpenAsyncSession(new SessionOptions 
            { 
                TransactionMode = TransactionMode.ClusterWide 
            });
            
            var users = await session.Advanced
                .AsyncRawQuery<User>("from Users where true and not exists(hasPatched)")
                .Take(batchSize)
                .ToArrayAsync();

            if (users.Length == 0)
            {
                Console.WriteLine("\nDone! There are no more users without the 'hasPatched' flag in the database.");
                break;
            }

            foreach (var user in users)
            {
                await session.StoreAsync(user);
            }
            
            await session.SaveChangesAsync();
            
            totalProcessed += users.Length;
            Console.WriteLine($"Processed in this run: {users.Length} | Total updated so far: {totalProcessed}");
            
            if (users.Length < batchSize)
            {
                Console.WriteLine("\nFinished processing all remaining users.");
                break;
            }
            
            bool validChoice = false;
            while (!validChoice)
            {
                Console.WriteLine("\nWhat would you like to do next?");
                Console.WriteLine($"[1] Process another {batchSize} documents");
                Console.WriteLine("[2] Change the number of documents to process");
                Console.WriteLine("[3] Exit");
                Console.Write("Select an option (1-3): ");

                var choice = Console.ReadLine();

                switch (choice)
                {
                    case "1":
                        validChoice = true;
                        break;
                    
                    case "2":
                        Console.Write("Enter the new number of documents to process: ");
                        var newSizeStr = Console.ReadLine();
                        if (int.TryParse(newSizeStr, out int newSize) && newSize > 0)
                        {
                            batchSize = newSize;
                            validChoice = true;
                        }
                        else
                        {
                            Console.WriteLine("Invalid input. Please try again.");
                        }
                        break;
                        
                    case "3":
                        store.OnAfterConversionToDocument -= OnAfterConversionToDocumentHandler;
                        Console.WriteLine($"\nExiting... Total number of updated documents: {totalProcessed}");
                        Console.WriteLine("Press any key to exit...");
                        Console.ReadKey();
                        return;
                        
                    default:
                        Console.WriteLine("Invalid choice. Please select 1, 2, or 3.");
                        break;
                }
            }
        }
        
        store.OnAfterConversionToDocument -= OnAfterConversionToDocumentHandler;

        Console.WriteLine($"\nPatching is complete. Total number of updated documents: {totalProcessed}");
        Console.WriteLine("Press any key to exit...");
        Console.ReadKey();
    }
    
    private static void OnAfterConversionToDocumentHandler(object sender, AfterConversionToDocumentEventArgs args)
    {
        const string hasPatched = "hasPatched";

        if (args.Entity is not User)
            return;

        var document = args.Document;
        if (document.TryGet(hasPatched, out object _))
            return;

        document.Modifications ??= new DynamicJsonValue(document);
        document.Modifications[hasPatched] = true;

        args.Document = args.Session.Context.ReadObject(document, args.Id);
    }
    
    public class User
    {
        public User()
        {
            Domains = new Dictionary<string, HashSet<string>>(StringComparer.OrdinalIgnoreCase);
            RootDomains = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            Emails = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
            LicenseIds = new HashSet<string>(StringComparer.OrdinalIgnoreCase);
        }

        public string Id { get; set; }

        public Dictionary<string, HashSet<string>> Domains { get; set; }

        public HashSet<string> RootDomains { get; set; }

        public HashSet<string> Emails { get; set; }

        public HashSet<string> LicenseIds { get; set; }

        public DateTimeOffset CreatedAt { get; set; }

        public string CloudAccountId { get; set; }
    }
}