#region Using directives
using System;
using UAManagedCore;
using OpcUa = UAManagedCore.OpcUa;
using FTOptix.UI;
using FTOptix.HMIProject;
using FTOptix.NativeUI;
using FTOptix.Retentivity;
using FTOptix.CoreBase;
using FTOptix.Core;
using FTOptix.NetLogic;
using System.DirectoryServices.AccountManagement;
#endregion

public class DomainLogic : BaseNetLogic
{
    public override void Start()
    {
        // Insert code to be executed when the user-defined logic is started
    }

    public override void Stop()
    {
        // Insert code to be executed when the user-defined logic is stopped
    }

    [ExportMethod]
    public void GetUserFullName()
    {
        string username = LogicObject.GetVariable("Username").Value;
        string domain = LogicObject.GetVariable("Domain").Value;
        var userFullName = Owner.GetVariable("UserFullName");

        if (username == "Anonymous")
        {
            userFullName.Value = "Anonymous";
        }
        else
        {
            using (var context = new PrincipalContext(ContextType.Domain, domain))
            using (var user = UserPrincipal.FindByIdentity(context, username))
            {
                if (user.DisplayName != null)
                    userFullName.Value = user.DisplayName;
                else
                    userFullName.Value = "";
            }
        }
    }
}
