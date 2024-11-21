using System;
using System.Collections.Generic;
using System.Linq;
using System.Web;
using System.Web.Services;

using System.Data;
using System.Data.SqlClient;
using System.ComponentModel;
using System.Globalization;
using System.Collections;
using System.Web.Configuration;

using System.Web.Script.Serialization;
using System.Text;
using DotNetNuke.Security;
using System.Web.Script.Services;
[System.Web.Script.Services.ScriptService]
public class wsShipTimesDR : System.Web.Services.WebService
{    
      SqlConnection sqlConn = new SqlConnection("Data Source=10.1.12.22;Initial Catalog=PB_reporting;Persist Security Info=True;User ID=pbReporting_user;Password=kB60Wu^U9R7");
    //SqlConnection sqlConn = new SqlConnection(System.Configuration.ConfigurationManager.ConnectionStrings["PublicReports.hc"].ToString());
    public DataSet GetDataSet(string sqlstr, string tab)
    {
        SqlDataAdapter sda = new SqlDataAdapter(sqlstr, sqlConn);
        DataSet ds = new DataSet();

        try
        {
            sqlConn.Open();
            sda.Fill(ds, tab);
        }
        catch (Exception ex) { }
        finally
        {
            sqlConn.Close();
        }
        return ds;
    }

    public string secureString(string input)
    {
        var objSecurity = new DotNetNuke.Security.PortalSecurity();
        string output;
        output = HttpContext.Current.Server.HtmlEncode(objSecurity.InputFilter(input, PortalSecurity.FilterFlag.NoSQL | PortalSecurity.FilterFlag.NoScripting | PortalSecurity.FilterFlag.NoMarkup));
        output = output.Replace("&#39;", "'");
        output = output.Replace("&amp;", "&");

        output = output.Replace("&#232;", "è");
        output = output.Replace("&#201;", "É");
        output = output.Replace("&#192;", "À");
        output = output.Replace("&#224;", "à");
        output = output.Replace("&#212;", "Ô");
        output = output.Replace("&#244;", "ô");
        output = output.Replace("&#219;", "Û");
        output = output.Replace("&#251;", "û");
        //output = output.Replace(";", "");


        if (output.Contains("''''")) { output = output.Replace("''''", "''"); }
        return output;
    }

    public string translateToFrench(string s)
    {
        string result = s;
        result = result.Replace("January", "Janvier"); result = result.Replace("February", "Février"); result = result.Replace("March", "Mars");
        result = result.Replace("April", "Avril"); result = result.Replace("May", "Mai"); result = result.Replace("June", "Juin");
        return result;
    }

    public DataTable getOrganizationByName(bool addDefault, string text, string language)
    {
        string param = secureString(text);
        StringBuilder sb = new StringBuilder();

        sb.Append(" Select Org_ID, OrganizationName, " + (language.Equals("fr") ? " dl.Location_Label_French " : "dl.Location_Label") + " as region , do.postalCode from DimOrganization do ");
        sb.Append(" inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ");
        sb.Append(" inner join dimlocation dl on do.Region_LocationID = dl.Location_ID ");
        sb.Append(" where dot.orgType_label = 'Warehouse Institution' and WT_PublicReporting = 1 and  OrganizationName like '%" + param + "%' order by OrganizationName");

        DataSet DS = new DataSet();
        DS = GetDataSet(sb.ToString(), "organization");

        if (addDefault)
        {
            DataRow dr = DS.Tables["organization"].NewRow();
            dr["Org_ID"] = "-1";
            dr["OrganizationName"] = "[Select Organization]";
            DS.Tables["organization"].Rows.InsertAt(dr, 0);
        }

        return DS.Tables["organization"];
    }

    public DataTable getCitiesByName(string text, string language)
    {
        string param = secureString(text);

        StringBuilder sb = new StringBuilder();

        sb.Append(" Select distinct [City_LocationID] , dl.Location_Label as city from DimOrganization do ");
        sb.Append(" inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ");
        sb.Append(" inner join dimlocation dl on do.City_LocationID = dl.Location_ID ");
        sb.Append(" where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and   dl.Location_Label like '%" + param + "%' order by  dl.Location_Label");

        DataSet DS = new DataSet();
        DS = GetDataSet(sb.ToString(), "cities");

        return DS.Tables["cities"];
    }

    public DataTable getAllOrganization(bool addDefault, string language)
    {
        string sqlString = "";

        sqlString = " Select Org_ID, OrganizationName, " + (language.Equals("fr") ? " dl.Location_Label_French " : "dl.Location_Label") + " as Region, dl2.Location_Label as city, OrgCategory_Label as HospType , do.postalCode from DimOrganization do ";
        sqlString += " inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ";
        sqlString += " inner join DimOrganizationCategory doc on doc.OrgCategory_ID = do.OrgCatID ";
        sqlString += " inner join dimlocation dl on do.Region_LocationID = dl.Location_ID ";
        sqlString += " inner join dimlocation dl2 on do.City_LocationID = dl2.Location_ID ";
        sqlString += " where dot.orgType_label = 'Warehouse Institution' and WT_PublicReporting = 1 order by OrganizationName";

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlString, "organization");

        if (addDefault)
        {
            DataRow dr = DS.Tables["organization"].NewRow();
            dr["Org_ID"] = "-1";
            dr["OrganizationName"] = "[Select Organization]";
            DS.Tables["organization"].Rows.InsertAt(dr, 0);
        }

        return DS.Tables["organization"];
    }

    public DataTable getOrganizationByID(bool addDefault, string text, string language)
    {
        string param = secureString(text);
        string sqlString = "";

        sqlString = " Select do.Org_ID, OrganizationName, " + (language.Equals("fr") ? " dl1.Location_Label_French " : "dl1.Location_Label") + " as Region, dl2.Location_Label as city, ";
        sqlString += " do.postalCode, do.website, do.fax, doc.OrgCategory_Label as WarehouseType  from DimOrganization do ";

        sqlString += " inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ";
        sqlString += " inner join DimOrganizationCategory doc on do.OrgCatID = doc.OrgCategory_ID ";
        sqlString += " inner join dimlocation dl1 on do.Region_LocationID = dl1.Location_ID ";
        sqlString += " inner join dimlocation dl2 on do.City_LocationID = dl2.Location_ID ";
        sqlString += " where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and do.Org_ID = " + param;

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlString, "organization");

        if (addDefault)
        {
            DataRow dr = DS.Tables["organization"].NewRow();
            dr["Org_ID"] = "-1";
            dr["OrganizationName"] = "[Select Organization]";
            DS.Tables["organization"].Rows.InsertAt(dr, 0);
        }

        return DS.Tables["organization"];
    }

    public DataTable getOrganizationFactsByID(string text, string datatable, string language)
    {

        string param = secureString(text);
        StringBuilder sqlSB = new StringBuilder();

        if (datatable.Equals("Fact_PS_CDI"))
        {

            sqlSB.Append(" Select top 1 dtp.timeperiod_label ,[Cases_reporting] , cast(ROUND([calc_Rate],2) as DECIMAL(28,2)) as calc_Rate, [Outbreak_Status] ");
        }
        else if (datatable.Equals("Fact_PS_MRSA") || datatable.Equals("Fact_PS_VRE"))
        {
            sqlSB.Append(" Select top 1 dtp.timeperiod_label ,[Cases_reporting] ,cast(ROUND([calc_Rate],3) as DECIMAL(28,3)) as calc_Rate  ");
        }
        else if (datatable.Equals("Fact_PS_CLI")
        || datatable.Equals("Fact_PS_VAP"))
        {
            sqlSB.Append(" Select top 1 dtp.timeperiod_label ,[Cases_reporting] ,cast(ROUND([calc_Rate],2) as DECIMAL(28,2)) as calc_Rate  ");
        }
        else if (datatable.Equals("Fact_PS_SSI") || datatable.Equals("Fact_PS_SSCL"))
        {
            sqlSB.Append(" Select top 1 dtp.timeperiod_label ,cast(ROUND([Percent],2) as DECIMAL(28,2)) as [Percent] ");
        }
        else if (datatable.Equals("Fact_PS_HH"))
        {
            sqlSB.Append(" Select top 1 dtp.timeperiod_label ,cast(ROUND([Percent_before],2) as DECIMAL(28,2)) as [Percent_before], cast(ROUND([Percent_after],2) as DECIMAL(28,2)) as [Percent_after] ");
        }
        sqlSB.Append(" from " + datatable + " fps ");
        sqlSB.Append(" inner join dimtimeperiod dtp on fps.TimePeriod_id = dtp.TimePeriod_id ");
        sqlSB.Append(" where fps.Org_ID = " + param);
        sqlSB.Append("order by dtp.TimePeriod_Date desc");

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlSB.ToString(), datatable);

        return DS.Tables[datatable];
    }

    public DataTable getAllOrganizationFactsByID(string text, string datatable, string language)
    {

        string param = secureString(text);
        StringBuilder sqlSB = new StringBuilder();

        if (datatable.Equals("Fact_PS_CDI"))
        {
            sqlSB.Append(" Select dtp.timeperiod_label ,[Cases_reporting] , cast(ROUND([calc_Rate],2) as DECIMAL(28,2)) as calc_Rate, [Outbreak_Status] ");
        }
        else if (datatable.Equals("Fact_PS_CLI") || datatable.Equals("Fact_PS_VAP"))
        {
            sqlSB.Append(" Select dtp.timeperiod_label ,[Cases_reporting] ,cast(ROUND([calc_Rate],2) as DECIMAL(28,2)) as calc_Rate  ");
        }
        else if (datatable.Equals("Fact_PS_MRSA") || datatable.Equals("Fact_PS_VRE"))
        {
            sqlSB.Append(" Select dtp.timeperiod_label ,[Cases_reporting] ,cast(ROUND([calc_Rate],3) as DECIMAL(28,3)) as calc_Rate  ");
        }
        else if (datatable.Equals("Fact_PS_SSI") || datatable.Equals("Fact_PS_SSCL"))
        {
            sqlSB.Append(" Select dtp.timeperiod_label ,cast(ROUND([Percent],2) as DECIMAL(28,2)) as [Percent] ");
        }
        else if (datatable.Equals("Fact_PS_HH"))
        {
            sqlSB.Append(" Select dtp.timeperiod_label ,cast(ROUND([Percent_before],2) as DECIMAL(28,2)) as [Percent_before], cast(ROUND([Percent_after],2) as DECIMAL(28,2)) as [Percent_after] ");
        }
        sqlSB.Append(" from " + datatable + " fps ");
        sqlSB.Append(" inner join dimtimeperiod dtp on fps.TimePeriod_id = dtp.TimePeriod_id ");
        sqlSB.Append(" where fps.Org_ID = " + param);
        sqlSB.Append("order by dtp.TimePeriod_Date desc");

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlSB.ToString(), datatable);

        return DS.Tables[datatable];
    }

    public DataTable getOrganizationByRegion(bool addDefault, string text, string language)
    {
        string param = secureString(text);
        string sqlString = "";

        sqlString = " Select Org_ID, OrganizationName, " + (language.Equals("fr") ? " dl.Location_Label_French " : "dl.Location_Label") + " as Region ,dl2.Location_Label as city, OrgCategory_Label as HospType, do.postalCode from DimOrganization do ";
        sqlString += " inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ";
        sqlString += " inner join DimOrganizationCategory doc on doc.OrgCategory_ID = do.OrgCatID ";
        sqlString += " inner join dimlocation dl on do.Region_LocationID = dl.Location_ID ";
        sqlString += " inner join dimlocation dl2 on do.City_LocationID = dl2.Location_ID ";
        sqlString += " where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and dl.location_code = '" + "Region" + param + "' order by OrganizationName";

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlString, "organization");

        if (addDefault)
        {
            DataRow dr = DS.Tables["organization"].NewRow();
            dr["Org_ID"] = "-1";
            dr["OrganizationName"] = "[Select Organization]";
            DS.Tables["organization"].Rows.InsertAt(dr, 0);
        }

        return DS.Tables["organization"];
    }


    public DataTable getOrganizationByWarehouseType(bool addDefault, string text, string language)
    {
        string param = secureString(text);
        string sqlString = "";

        sqlString = " Select Org_ID, OrganizationName, " + (language.Equals("fr") ? " dl.Location_Label_French " : "dl.Location_Label") + " as Region ,dl2.Location_Label as city, OrgCategory_Label as HospType, do.postalCode from DimOrganization do ";
        sqlString += " inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ";
        sqlString += " inner join DimOrganizationCategory doc on doc.OrgCategory_ID = do.OrgCatID ";
        sqlString += " inner join dimlocation dl on do.Region_LocationID = dl.Location_ID ";
        sqlString += " inner join dimlocation dl2 on do.City_LocationID = dl2.Location_ID ";
        sqlString += " where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and doc.OrgCategory_ID = '" + param + "' order by OrganizationName";

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlString, "organization");

        if (addDefault)
        {
            DataRow dr = DS.Tables["organization"].NewRow();
            dr["Org_ID"] = "-1";
            dr["OrganizationName"] = "[Select Organization]";
            DS.Tables["organization"].Rows.InsertAt(dr, 0);
        }

        return DS.Tables["organization"];
    }

    public DataTable getOrganizationByInd(string ind, string hospType)
    {
        string param = secureString(hospType);
        string indicator = secureString(ind);
        string columnName = "";
        StringBuilder sb = new StringBuilder();
        Int16 RoundAs = 2;
        if (ind.ToLower().Equals("cdi") || ind.ToLower().Equals("cli") || ind.ToLower().Equals("mrsa") || ind.ToLower().Equals("vap") || ind.ToLower().Equals("vre")) { columnName = "[calc_Rate]"; }
        else if (ind.ToLower().Equals("ssi") || ind.ToLower().Equals("sscl")) { columnName = "[Percent]"; }
        else if (ind.ToLower().Equals("hh")) { columnName = "[Percent_after]"; }

        if (ind.ToLower().Equals("mrsa") || ind.ToLower().Equals("vre")) RoundAs = 3; else RoundAs = 2;

        sb.Append(" Select distinct top 2 dtp.TimePeriod_Label, TimePeriod_Date from Fact_PS_" + indicator + " fps ");
        sb.Append(" inner join DimTimePeriod dtp on fps.timePeriod_id = dtp.timePeriod_id ");
        sb.Append(" order by TimePeriod_Date desc ");

        DataSet DS1 = new DataSet();
        DS1 = GetDataSet(sb.ToString(), "timePeriods");

        sb.Length = 0;
        DataRow lastRow = DS1.Tables["timePeriods"].Rows[DS1.Tables["timePeriods"].Rows.Count - 1];

        sb.Append(" Select do.Org_ID, OrganizationName,  ");

        foreach (DataRow dr in DS1.Tables["timePeriods"].Rows)
        {
            if (!object.ReferenceEquals(dr, lastRow))
            {
                sb.Append(" SUM(CASE timePeriod_Label WHEN '" + dr["TimePeriod_Label"] + "'  THEN cast(ROUND(" + columnName + "," + RoundAs + ") as DECIMAL(28," + RoundAs + ")) END) as [" + dr["TimePeriod_Label"] + "] , ");
            }
            else sb.Append(" SUM(CASE timePeriod_Label WHEN '" + dr["TimePeriod_Label"] + "'  THEN cast(ROUND(" + columnName + "," + RoundAs + ") as DECIMAL(28," + RoundAs + ")) END) as [" + dr["TimePeriod_Label"] + "] ");
            //if (!object.ReferenceEquals(dr, lastRow)){
            //    sb.Append(" SUM(CASE timePeriod_Label WHEN '" + dr["TimePeriod_Label"] + "'  THEN cast(ROUND(" + columnName + ",2) as DECIMAL(28,2)) END) as [" + dr["TimePeriod_Label"] + "] , ");
            //}
            //else sb.Append(" SUM(CASE timePeriod_Label WHEN '" + dr["TimePeriod_Label"] + "'  THEN cast(ROUND(" + columnName + ",2) as DECIMAL(28,2)) END) as [" + dr["TimePeriod_Label"] + "] ");
        }

        sb.Append(" from DimOrganization do ");
        sb.Append(" inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ");
        sb.Append(" inner join DimOrganizationCategory doc on do.OrgCatID = doc.OrgCategory_ID ");
        sb.Append(" inner join Fact_PS_" + indicator + " fps on fps.org_id = do.org_id ");
        sb.Append(" inner join DimTimePeriod dtp on fps.timePeriod_id = dtp.timePeriod_id ");
        sb.Append(" where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and do.OrgCatID = " + param);
        sb.Append(" group by do.Org_ID, OrganizationName ");
        sb.Append(" order by OrganizationName ");

        DataSet DS = new DataSet();
        DS = GetDataSet(sb.ToString(), "organization");

        return DS.Tables["organization"];
    }

    public DataTable getOrganizationByPostalCode(bool addDefault, string language)
    {
        string sqlString = "";

        sqlString = " Select Org_ID, OrganizationName, dl.Location_Label  as city, " + (language.Equals("fr") ? " dl.Location_Label_French " : "dl.Location_Label") + " as Region, do.postalCode,OrgCategory_Label as HospType, longitude, latitude from DimOrganization do ";
        sqlString += " inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ";
        sqlString += " inner join DimOrganizationCategory doc on doc.OrgCategory_ID = do.OrgCatID ";
        sqlString += " inner join dimlocation dl on do.Region_LocationID = dl.Location_ID ";
        sqlString += " where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and longitude is not null and latitude is not null ";

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlString, "organization");

        if (addDefault)
        {
            DataRow dr = DS.Tables["organization"].NewRow();
            dr["Org_ID"] = "-1";
            dr["OrganizationName"] = "[Select Organization]";
            DS.Tables["organization"].Rows.InsertAt(dr, 0);
        }

        return DS.Tables["organization"];
    }

    public DataTable getOrganizationByCity(string param, string language)
    {
        string sqlString = "";

        sqlString = " Select Org_ID, OrganizationName, dl1.Location_Label  as city, dl2.Location_Label  as Region, OrgCategory_Label as HospType, do.postalCode from DimOrganization do ";
        sqlString += " inner join DimOrgType dot on do.OrgType_ID = dot.OrgType_ID ";
        sqlString += " inner join DimOrganizationCategory doc on doc.OrgCategory_ID = do.OrgCatID ";
        sqlString += " inner join dimlocation dl1 on do.City_LocationID = dl1.Location_ID ";
        sqlString += " inner join dimlocation dl2 on do.Region_LocationID = dl2.Location_ID ";
        sqlString += " where dot.orgType_label = 'Warehouse Institution' and do.WT_PublicReporting = 1 and dl1.Location_Label = '" + param + "' order by OrganizationName ";

        DataSet DS = new DataSet();
        DS = GetDataSet(sqlString, "organization");

        return DS.Tables["organization"];
    }

    public DataTable getPostalCode(string sPC)
    {
        string param = secureString(sPC);
        string sqlString = "";
        DataSet DS = new DataSet();

        param = param.Trim();
        param = param.Replace("_", "");
        param = param.Replace("-", "");
        param = param.Replace(" ", "");

        param = param.Insert(3, " ");
        sqlString = " Select Latitude, Longitude from PostalCodes ";
        sqlString += " where PostalCode = '" + param + "'";

        DS = GetDataSet(sqlString, "sLongLat");

        return DS.Tables["sLongLat"];
    }

    [WebMethod]
    public string getTableDataByName(string q, string language)
    {
        string param = secureString(q);
        StringBuilder sb = new StringBuilder();
        DataTable orgs = getOrganizationByName(false, param, language);
        List<warehouse> WarehouseNames = new List<warehouse>();

        sb.Append("[");
        DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];
        foreach (DataRow r in orgs.Rows)
        {
            if (!object.ReferenceEquals(r, lastRow))
            {
                sb.Append("[" + "\"<a href='javascript:displayIndividualWarehouseInfo(" + r["Org_ID"].ToString() + ")'>" + r["OrganizationName"].ToString().Replace("\"", "''") + "</a>\"" + "," + "\"" + r["Region"].ToString() + "\"" + "," + "\"" + r["postalcode"].ToString() + "\"" + "],");
            }
            else { sb.Append("[" + "\"<a href='javascript:displayIndividualWarehouseInfo(" + r["Org_ID"].ToString() + ")'>" + r["OrganizationName"].ToString().Replace("\"", "''") + "</a>\"" + "," + "\"" + r["Region"].ToString() + "\"" + "," + "\"" + r["postalcode"].ToString() + "\"" + "]"); }
        }
        sb.Append("]");

        return sb.ToString();
    }

    [WebMethod]
    public string getTableDataAllWarehouses(string language)
    {

        StringBuilder sb = new StringBuilder();
        DataTable orgs = getAllOrganization(false, language);
        List<warehouse> WarehouseNames = new List<warehouse>();

        sb.Append("[");
        DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];
        foreach (DataRow r in orgs.Rows)
        {
           string orgID = r["Org_ID"].ToString();
           //////if (!object.ReferenceEquals(r, lastRow))
           //////{
           //////    sb.Append("[" + "\"<input type='checkbox'  name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' >\"" + "," + "\"<a href='#' class='AllWarehousesDisplayIndividualWarehouseInfo'  data-OrgName='" + r["OrganizationName"].ToString().Replace("\"", "''") + "' data-OrgID='" + orgID + "'>" + r["OrganizationName"].ToString().Replace("\"", "''") + "</a>\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["Region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "],");
           //////}
           //////else { sb.Append("[" + "\"<input type='checkbox'   name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "'  data-OrgID='" + orgID + "' >\"" + "," + "\"<a href='#' class='AllWarehousesDisplayIndividualWarehouseInfo'  data-OrgName='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'  data-OrgID='" + orgID + "'>" + r["OrganizationName"].ToString().Replace("\"", "''") + "</a>\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["Region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "]"); }

           if (!object.ReferenceEquals(r, lastRow))
           {
               sb.Append("[" + "\"<input type='checkbox'  name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-Region='" + r["Region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["Region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "],");
           }
           else { sb.Append("[" + "\"<input type='checkbox'  name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-Region='" + r["Region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["Region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "]"); }
      
        }
        sb.Append("]");

        return sb.ToString();
    }

    [WebMethod]
    public warehouse getIndividualWarehouseInfo(string q, string language)
    {
        string param = secureString(q);
        DataTable tbCopy = new DataTable();
        string[] datatables = { "Fact_PS_CDI", "Fact_PS_CLI", "Fact_PS_HH", "Fact_PS_MRSA", "Fact_PS_SSCL", "Fact_PS_SSI", "Fact_PS_VAP", "Fact_PS_VRE" };
        warehouse Warehouse = null;
        DataTable org = getOrganizationByID(false, param, language);
        DataSet factDS = new DataSet();

        foreach (string datatable in datatables)
        {
            tbCopy = getOrganizationFactsByID(param, datatable, language).Copy();
            factDS.Tables.Add(tbCopy);
        }

        if (org.Rows.Count > 0)
        {
            DataRow r1 = org.Rows[0];
            Warehouse = new warehouse()
            {
                ID = r1["Org_ID"].ToString(),
                Name = r1["OrganizationName"].ToString(),
                WarehouseType = r1["WarehouseType"].ToString(),
                Region = r1["region"].ToString(),
                city = r1["city"].ToString(),
                postalCode = r1["postalCode"].ToString(),
                website = r1["website"].ToString()
            };
        }

        if (factDS.Tables.Count > 0)
        {
            if (factDS.Tables["Fact_PS_CDI"] != null)
            {
                string outbreakMessage = "";
                string outbreakStatus = factDS.Tables["Fact_PS_CDI"].Rows[0]["Outbreak_Status"].ToString();
                if ((!outbreakStatus.Equals("")) && (!outbreakStatus.Equals(" "))) outbreakMessage = "<div><span>Outbreak Status: " + outbreakStatus + ", please contact the warehouse for more information</span></div>";
                Warehouse.tbl_cdi += outbreakMessage + "<table><thead><tr><th scope='col'>Reporting Period</th><th scope='col'>Rate per 1,000 patient days</th><th scope='col'>Case count</th></tr></thead><tbody>";
                foreach (DataRow dr in factDS.Tables["Fact_PS_CDI"].Rows)
                {
                    Warehouse.tbl_cdi += " <tr><td style='width:150px'>" + dr["timeperiod_label"].ToString() + "</td><td>" + replaceFactCodes(dr["calc_Rate"].ToString(), false) + "</td><td>" + replaceFactCodes(dr["Cases_reporting"].ToString(), false) + "</td></tr> ";
                }
                Warehouse.tbl_cdi += "</tbody><tfoot><tr><td colspan=4><a href='javascript:displayIndividualWarehouseInfoByInd(" + Warehouse.ID + ", \"CDI\")'>All reporting periods</a><br/><a href='./searchByWarehouseType.html?ht=" + convertHospType(Warehouse.WarehouseType) + "&ind=cdi'>Compare with other warehouses of the same type</a><br/><a href='../../completeReports/cdi.xls'>Download Historical Data</a></td></tr></tfoot></table>";
            }

            //filling mrsa
            if (factDS.Tables["Fact_PS_MRSA"] != null)
            {
                Warehouse.tbl_mrsa += "<table><thead><tr><th scope='col'>Reporting Period</th><th scope='col'>Rate per 1,000 patient days</th><th scope='col'>Case count</th></tr></thead><tbody>";
                foreach (DataRow dr in factDS.Tables["Fact_PS_MRSA"].Rows)
                {
                    Warehouse.tbl_mrsa += " <tr><td style='width:150px'>" + dr["timeperiod_label"].ToString() + "</td><td>" + replaceFactCodes(dr["calc_Rate"].ToString(), false) + "</td><td>" + replaceFactCodes(dr["Cases_reporting"].ToString(), false) + "</td></tr> ";
                }
                Warehouse.tbl_mrsa += "</tbody><tfoot><tr><td colspan=4><a href='javascript:displayIndividualWarehouseInfoByInd(" + Warehouse.ID + ", \"MRSA\")'>All reporting periods</a><br/><a href='./searchByWarehouseType.html?ht=" + convertHospType(Warehouse.WarehouseType) + "&ind=mrsa'>Compare with other warehouses of the same type</a><br/><a href='../../completeReports/mrsa.xls'>Download Historical Data</a></td></tr></tfoot></table>";
            }            
        return Warehouse;
    }    

    [WebMethod]
    public List<warehouse> GetCities(string q, string language)
    {
        string param = secureString(q);
        DataTable orgs = getCitiesByName(param, language);
        List<warehouse> WarehouseNames = new List<warehouse>();

        foreach (DataRow r in orgs.Rows)
        {
            WarehouseNames.Add(new warehouse() { ID = r["City_LocationID"].ToString(), Name = r["city"].ToString() });
        }

        return WarehouseNames;
    }

    [WebMethod]
    public List<warehouse> GetWarehouseNamesBypostalCode(string q, string language)
    {
        string param = secureString(q);
        DataTable orgs = getOrganizationByPostalCode(false, language);
        List<warehouse> WarehouseNames = new List<warehouse>();
        //WarehouseNames.Sort();

        foreach (DataRow r in orgs.Rows)
        {
            WarehouseNames.Add(new warehouse() { ID = r["Org_ID"].ToString(), Name = r["OrganizationName"].ToString() });
        }

        return WarehouseNames;
    }

    [WebMethod]
    public List<warehouse> GetWarehouseNamesByCity(string q, string language)
    {
        string param = secureString(q);
        DataTable orgs = getOrganizationByCity(param, language);
        List<warehouse> WarehouseNames = new List<warehouse>();
        //WarehouseNames.Sort();

        foreach (DataRow r in orgs.Rows)
        {
            WarehouseNames.Add(new warehouse() { ID = r["Org_ID"].ToString(), Name = r["OrganizationName"].ToString() });
        }

        return WarehouseNames;
    }

    [WebMethod]
    public string getTableDataByPostalCode(string q, string language)
    {
        string param = secureString(q);
        Double longitude1 = 0;
        Double latitude1 = 0;
        Double distance = new Double();
        Double longitude2 = 0;
        Double latitude2 = 0;
        StringBuilder sb = new StringBuilder();

        DataTable sourcePC = getPostalCode(param);

        if (sourcePC.Rows.Count > 0)
        {

            longitude1 = double.Parse(sourcePC.Rows[0]["Longitude"].ToString());
            latitude1 = double.Parse(sourcePC.Rows[0]["Latitude"].ToString());

            DataTable orgs = getOrganizationByPostalCode(false, language);
            List<warehouse> Warehouses = new List<warehouse>();

            sb.Append("[");
            DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];
            foreach (DataRow r in orgs.Rows)
            {
                if ((!r["longitude"].ToString().Equals("")) && (!r["latitude"].ToString().Equals("")))
                {
                    try
                    {
                        longitude2 = Double.Parse(r["longitude"].ToString());
                        latitude2 = Double.Parse(r["latitude"].ToString());
                    }
                    catch (Exception ex)
                    {

                    }

                    distance = PS_DistanceAlgorithm.CalcDistanceBetweenPlaces(latitude1, longitude1, latitude2, longitude2);   //double Lat1,double Long1, double Lat2, double Long2
                    distance = Math.Round((2 * Math.Pow(Math.Pow(distance, 2) / 2, 0.5)), 1); // distance correction from straight dist. to manhattan dist.

                    ///original
                    ////  Warehouses.Add(new warehouse() {  ID = r["Org_ID"].ToString(), Name = r["OrganizationName"].ToString().Replace("\"", "''"), Region = r["region"].ToString(), WarehouseType = r["HospType"].ToString(), postalCode = r["postalcode"].ToString(), distanceToPostalCode = distance });
                    Warehouses.Add(new warehouse() { ID = r["Org_ID"].ToString(), city = r["city"].ToString(), Name = r["OrganizationName"].ToString().Replace("\"", "''"), Region = r["region"].ToString(), WarehouseType = r["HospType"].ToString(), postalCode = r["postalcode"].ToString(), distanceToPostalCode = distance });
                }
            }

            Warehouses.Sort(
                        delegate(warehouse x, warehouse y)
                        {
                            if (x == null)
                            {
                                if (y == null) { return 0; }
                                return -1;
                            }
                            if (y == null) { return 0; }
                            return x.distanceToPostalCode.CompareTo(y.distanceToPostalCode);
                        }
                    );

            for (int i = 0; i < 20 && i < Warehouses.Count; i++)
            {

                string orgID = Warehouses[i].ID.ToString();
                sb.Append("[" + "\"<input type='checkbox'  name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + Warehouses[i].postalCode.ToString() + "' data-WarehouseType='" + Warehouses[i].WarehouseType.ToString() + "' data-region='" + Warehouses[i].Region.ToString() + "' data-city='" + Warehouses[i].city.ToString() + "' data-name='" + Warehouses[i].Name.ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + Warehouses[i].Name.ToString().Replace("\"", "''") + "\"" + "," + "\"" + Warehouses[i].Region.ToString() + "\"" + "," + "\"" + Warehouses[i].WarehouseType.ToString() + "\"" + "," + "\"" + Warehouses[i].distanceToPostalCode + "\"" + "],");



            }
            sb.Remove(sb.Length - 1, 1);
            sb.Append("]");

        }
        return sb.ToString();
    }

    [WebMethod]
    public string getTableDataByWarehouseType(string q, string language)
    {
        string param = secureString(q);
        StringBuilder sb = new StringBuilder();
        DataTable orgs = getOrganizationByWarehouseType(false, param, language);
        List<warehouse> WarehouseNames = new List<warehouse>();

        sb.Append("[");

        //original
        // DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];
        DataRow lastRow = null;
        bool orgRowCount = false;
        if (orgs.Rows.Count > 0)
        {
            lastRow = orgs.Rows[orgs.Rows.Count - 1];
            orgRowCount = true;
        }


        if (orgRowCount)
        {
            foreach (DataRow r in orgs.Rows)
            {
                string orgID = r["Org_ID"].ToString();
                if (!object.ReferenceEquals(r, lastRow))
                {
                    sb.Append("[" + "\"<input type='checkbox'  name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-region='" + r["region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "],");
                }
                else { sb.Append("[" + "\"<input type='checkbox'  name='orgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-region='" + r["region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "]"); }
      
            }
        }

        sb.Append("]");

        return sb.ToString();
    }

    [WebMethod]
    public string getTableDataByRegion(string q, string language)
    {
        string param = secureString(q);
        StringBuilder sb = new StringBuilder();
        DataTable orgs = getOrganizationByRegion(false, param, language);
        List<warehouse> WarehouseNames = new List<warehouse>();

        sb.Append("[");

        //original
        // DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];
        DataRow lastRow = null;
        bool orgRowCount = false;
        if (orgs.Rows.Count > 0)
        {
            lastRow = orgs.Rows[orgs.Rows.Count - 1];
            orgRowCount = true;
        }


        if (orgRowCount)
        {
            foreach (DataRow r in orgs.Rows)
            {
                string orgID = r["Org_ID"].ToString();
                if (!object.ReferenceEquals(r, lastRow))
                {
                    sb.Append("[" + "\"<input type='checkbox'  name='AAAorgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-region='" + r["region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "],");
                }
                else { sb.Append("[" + "\"<input type='checkbox'  name='AAAorgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-region='" + r["region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "]"); }
      
            }
        }

        sb.Append("]");

        return sb.ToString();
    }

    [WebMethod]
    public string getTableDataByCity(string q, string language)
    {

        string param = secureString(q);
        StringBuilder sb = new StringBuilder();
        DataTable orgs = getOrganizationByCity(param, language);
        List<warehouse> WarehouseNames = new List<warehouse>();

        sb.Append("[");
        DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];
        foreach (DataRow r in orgs.Rows)
        {
           
            string orgID = r["Org_ID"].ToString();
            if (!object.ReferenceEquals(r, lastRow))
            {
                sb.Append("[" + "\"<input type='checkbox'  name='AAAorgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-region='" + r["region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "],");
            }
            else { sb.Append("[" + "\"<input type='checkbox'  name='AAAorgid" + orgID + "' id='orgid" + orgID + "' value='" + orgID + "' data-OrgID='" + orgID + "' data-postalCode='" + r["postalCode"].ToString() + "' data-WarehouseType='" + r["HospType"].ToString() + "' data-region='" + r["region"].ToString() + "' data-city='" + r["city"].ToString() + "' data-name='" + r["OrganizationName"].ToString().Replace("\"", "''") + "'>\"" + "," + "\"" + r["OrganizationName"].ToString().Replace("\"", "''") + "\"" + "," + "\"" + r["city"].ToString() + "\"" + "," + "\"" + r["region"].ToString() + "\"" + "," + "\"" + r["HospType"].ToString() + "\"" + "]"); }
      
        }
        sb.Append("]");

        return sb.ToString();
    }

    [WebMethod]
    public string getTableDataByInd(string indicator, string q, string language)
    {
        StringBuilder sb = new StringBuilder();
        DataTable orgs = getOrganizationByInd(indicator, q);
        List<warehouse> WarehouseNames = new List<warehouse>();
        int i = 0;

        sb.Append("[");
        int columsCount = orgs.Columns.Count;
        DataRow lastRow = orgs.Rows[orgs.Rows.Count - 1];

        sb.Append("[\"" + orgs.Columns[2].ColumnName + "\"],[\"" + orgs.Columns[3].ColumnName + "\"],");

        foreach (DataRow r in orgs.Rows)
        {
            if (!object.ReferenceEquals(r, lastRow))
            {
                sb.Append("[" + "\"<a href='javascript:ByTopicDisplayIndividualWarehouseInfo(" + r["Org_ID"].ToString() + ")'>" + r["OrganizationName"].ToString().Replace("\"", "''") + "</a>\"");
                for (i = 2; i < columsCount; i++)
                {
                    sb.Append("," + "\"" + (r[i].Equals(DBNull.Value) ? "Not Required to Report" : replaceFactCodes(r[i].ToString(), true)) + "\"");
                }
                sb.Append("],");
            }
            else
            {
                sb.Append("[" + "\"<a href='javascript:ByTopicDisplayIndividualWarehouseInfo(" + r["Org_ID"].ToString() + ")'>" + r["OrganizationName"].ToString().Replace("\"", "''") + "</a>\"");
                for (i = 2; i < columsCount; i++)
                {
                    sb.Append("," + "\"" + (r[i].Equals(DBNull.Value) ? "Not Required to Report" : replaceFactCodes(r[i].ToString(), true)) + "\"");
                }
                sb.Append("]");
            }
        }
        sb.Append("]");

        if (language.Equals("fr")) { return translateToFrench(sb.ToString()); }
        else { return sb.ToString(); }
    }
}