package googledrive4go

import (
	"context"
	"fmt"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"path"
	"strings"
	"sync"
	"time"
)

func Builder(client *http.Client, subject string, ctx *context.Context) *GoogleDriveAPI {
	service, err := drive.NewService(*ctx, option.WithHTTPClient(client))
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	log.Printf("Initialized GoogleDrive4Go as (%s)\n", subject)
	return &GoogleDriveAPI{Service: service, Subject: subject}
}

type GoogleDriveAPI struct {
	Service *drive.Service
	Subject string
}

type DriveFile struct {
	FileInfo       os.FileInfo
	DriveInfo      *drive.File
	FilePath       string
	Blob           []byte
	FileExtension  string
	FullFileName   string
	OriginalFileID string
}

func (receiver *GoogleDriveAPI) GetAbout() *drive.About {
	log.Printf("Getting Drive.About of [%s]\n", receiver.Subject)
	response, err := receiver.Service.About.Get().Fields("*").Do()
	if err != nil {
		log.Println(err.Error())
		return nil
	}
	return response
}

/*Files*/

func (receiver *GoogleDriveAPI) GetFileById(fileId string) *drive.File {
	file, err := receiver.Service.Files.Get(fileId).Fields("*").Do()
	if err != nil {
		if strings.Contains(err.Error(), "File not found:") {
			log.Println(err.Error())
			return nil
		}
		log.Println(err.Error())
		log.Println("Error encountered Sleeping for 30 seconds...")
		time.Sleep(time.Second * 30)
		return receiver.GetFileById(fileId)
	}
	log.Printf("Returned [%s] -> \"%s\"\n", fileId, file.Name)
	return file
}

func (receiver *GoogleDriveAPI) QueryFiles(q string) []*drive.File {
	var allFiles []*drive.File
	request := receiver.Service.Files.List().Q(q).Fields("*").PageSize(1000)

	for {
		response, err := request.Do()
		if err != nil {
			log.Println(err.Error())
			if strings.Contains(err.Error(), "500") {
				log.Println("Backing off for 30 seconds...")
				time.Sleep(time.Second * 30)
				response, _ = request.Do()
			} else {
				log.Println(err.Error())
				return allFiles
			}
		}
		allFiles = append(allFiles, response.Files...)
		request.PageToken(response.NextPageToken)
		log.Printf("User: %s, Query: %s, Total returned: %d \n", receiver.Subject, q, len(allFiles))
		if response.NextPageToken == "" {
			break
		}
	}

	return allFiles
}

func (receiver *GoogleDriveAPI) MoveFile(fileId, parentFolderId string) *drive.File {
	updatedDriveFile, err := receiver.Service.Files.Update(
		fileId,
		&drive.File{}).
		AddParents(parentFolderId).Do()
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}
	log.Printf("Drive file [%s] moved to --> [%s]\n", fileId, parentFolderId)
	return updatedDriveFile
}

func (receiver *GoogleDriveAPI) CopyFile(fileId, parentFolderId, fileName string) *drive.File {
	msg := "Copy of [" + fileId + "]"
	response, err := receiver.Service.Files.Copy(fileId, &drive.File{Parents: []string{parentFolderId}}).Do()
	if err != nil {
		log.Println(msg + " FAILED...")
		if strings.Contains(err.Error(), "This file cannot be copied by the user.") {
			log.Printf("%s\n\tFile Id: %s\n\tFile Name: %s\n\tFile Location: %s\n\n", err.Error(), fileId, fileName, parentFolderId)
			return nil
		}
		log.Printf("%s\nSleeping for 3 seconds...", err.Error())
		time.Sleep(time.Second * 2)
		return receiver.CopyFile(fileId, parentFolderId, fileName)

	}
	log.Println(msg+response.Name, "SUCCESS...")
	return response
}

func (receiver *GoogleDriveAPI) ChangeFileOwner(newOwner, fileId string, doit bool) *drive.Permission {
	newPermission := &drive.Permission{}
	newPermission.EmailAddress = newOwner
	newPermission.Role = "owner"
	newPermission.Type = "user"
	changeOwnerRequest := receiver.Service.Permissions.Create(fileId, newPermission).TransferOwnership(true)
	msg := "File [" + fileId + "] old owner [" + receiver.Subject + "] -> new owner [" + newOwner + "] "
	if doit {
		response, err := changeOwnerRequest.Do()
		if err != nil {
			if strings.Contains(err.Error(), "Sorry, the items were successfully shared but emails could not be sent to") {
				log.Println(msg + "SUCCESS - Ownership change email not sent")
				return response
			}
			if strings.Contains(err.Error(), "some error code") {
				log.Println(err.Error())
				log.Println(msg + "FAILED - Retrying")
				time.Sleep(3 * time.Second)
				return receiver.ChangeFileOwner(newOwner, fileId, doit)
			} else {
				log.Println(msg + "FAILED\n\t" + err.Error())
				return nil
			}
		}
		log.Println(msg + "SUCCESS")
		return response
	} else {
		log.Println(msg + " DID NOT EXECUTE")
		return nil
	}

}

func (receiver *GoogleDriveAPI) ChangeFileOwnerWorker(newOwner, fileId string, doit bool, wg *sync.WaitGroup) {
	receiver.ChangeFileOwner(newOwner, fileId, doit)
	wg.Done()
}

func (receiver *GoogleDriveAPI) UploadFile(absoluteFilePath, parentFolderId string) (*drive.File, error) {
	byteCount := func(b int64) string {
		const unit = 1000
		if b < unit {
			return fmt.Sprintf("%d B", b)
		}
		div, exp := int64(unit), 0
		for n := b / unit; n >= unit; n /= unit {
			div *= unit
			exp++
		}
		return fmt.Sprintf("%.1f %cB",
			float64(b)/float64(div), "kMGTPE"[exp])
	}
	reader, err := os.Open(absoluteFilePath)
	if err != nil {
		panic(err)
	}
	fileInfo, _ := reader.Stat()
	var metaData = &drive.File{Name: fileInfo.Name()}
	if parentFolderId != "" {
		var parents []string
		parents = append(parents, parentFolderId)
		metaData.Parents = parents
	}
	progressUpdater := googleapi.ProgressUpdater(func(now, size int64) {
		log.Println("CurrentFile:",
			absoluteFilePath,
			"["+byteCount(now), "of", byteCount(fileInfo.Size())+"]")
	})
	result, err := receiver.Service.Files.Create(metaData).Media(reader).ProgressUpdater(progressUpdater).Do()
	reader.Close()
	return result, err
}

/*Folders*/
func (receiver *GoogleDriveAPI) CopyFolder(sourceFolderId, newSourceFolderName, parentFolderId string) {

	/*Get source folder*/
	sourceFolder := receiver.GetFileById(sourceFolderId)
	sourceFolder.Name = newSourceFolderName
	msg := "Copy of [" + sourceFolder.Name + "]"

	/*Create a copy source folder*/
	sourceCopy := receiver.CreateFolder(sourceFolder.Name, parentFolderId, nil, false)

	/*FileIdList that will be copied*/
	var filesToCopy [][]string
	var copyMap = make(map[string]string)

	/*Get all kids from SourceFolder*/
	for _, currentObject := range receiver.QueryFiles("'" + sourceFolder.Id + "' in parents") {
		if strings.Contains(currentObject.MimeType, "folder") {
			/*If file is a folder, copy that folder and play it in the current folder*/
			receiver.CopyFolder(currentObject.Id, currentObject.Name, sourceCopy.Id)
			log.Println(msg + " SUCCESS...")
			continue
		} else if strings.Contains(currentObject.MimeType, "shortcut") { // Added: 3/18/2021
			receiver.Service.Files.Get(currentObject.Id).Fields()
		}
		//CopyFile(currentObject.Id, parentFolderId)
		copyMap[currentObject.Id] = parentFolderId
		filesToCopy = append(filesToCopy, []string{currentObject.Id, sourceCopy.Id, currentObject.Name})
	}

	totalItems := len(filesToCopy) //Total Work Items
	maxGoRoutines := 10            //Max GoRoutines
	counter := 0                   //Counter

	for len(filesToCopy) != 0 {
		log.Println("Working [" + fmt.Sprint(counter) + "] of [" + fmt.Sprint(totalItems) + "]")
		if len(filesToCopy) < maxGoRoutines {
			currentItems := filesToCopy[:]
			waitgroup := sync.WaitGroup{}
			waitgroup.Add(len(currentItems))
			for _, item := range currentItems {
				go receiver.CopyFileWorker(item, &waitgroup)
				counter++
			}
			waitgroup.Wait()
			break
		} else {
			currentItems := filesToCopy[:maxGoRoutines]
			waitgroup := sync.WaitGroup{}
			waitgroup.Add(len(currentItems))
			for _, item := range currentItems {
				go receiver.CopyFileWorker(item, &waitgroup)
				counter++
			}
			waitgroup.Wait()
			filesToCopy = append(filesToCopy[:0], filesToCopy[maxGoRoutines:]...)
		}
	}
}

func (receiver *GoogleDriveAPI) CreateFolder(folderName, parentFolderId string, permissions []*drive.Permission, restricted bool) *drive.File {
	file := &drive.File{}
	file.MimeType = "application/vnd.google-apps.folder"
	file.Name = folderName
	if parentFolderId != "" {
		file.Parents = []string{parentFolderId}
	}

	driveFileCreateResponse, filesCreateErr := receiver.Service.Files.Create(file).Do()
	if filesCreateErr != nil {
		if strings.Contains(filesCreateErr.Error(), "limit") {
			log.Println(filesCreateErr.Error())
			log.Println("Api limit reached. Sleeping for 2 seconds...")
			time.Sleep(time.Second * 2)
			return driveFileCreateResponse
		}
	}

	if permissions != nil {
		for _, permission := range permissions {
			permissionResponse, err := receiver.Service.Permissions.Create(driveFileCreateResponse.Id, permission).SendNotificationEmail(false).Do()
			if err != nil {
				log.Println(err.Error())
			} else {
				log.Printf("Shared \"%s\" [%s] to <%s> as a {%s}", driveFileCreateResponse.Name, driveFileCreateResponse.Id, permission.EmailAddress, permissionResponse.Role)
			}
		}
	}

	log.Printf("Created folder %s[%s]", driveFileCreateResponse.Name, driveFileCreateResponse.Id)
	return driveFileCreateResponse
}

func (receiver *GoogleDriveAPI) GetNestedFiles(targetFolderId string) []*drive.File {
	targetFolder := receiver.GetFileById(targetFolderId)
	log.Println("Pulling Children from folder [" + targetFolder.Id + "] - " + targetFolder.Name)
	files := receiver.QueryFiles("'" + targetFolder.Id + "' in parents")
	if files == nil {
		log.Println("No files found in [" + targetFolder.Id + "]")
		return nil
	}
	var fileList []*drive.File
	for _, file := range files {
		log.Printf("CurrentFile: %s, {%s} - [%s]", file.Name, file.MimeType, file.Id)
		//Append data and keep going if folder
		if file.MimeType == "application/vnd.google-apps.folder" {
			fileList = append(fileList, receiver.GetNestedFiles(file.Id)...)
		}
		fileList = append(fileList, file)
	}

	return fileList
}

/*Sharing*/
func (receiver *GoogleDriveAPI) GetFilePermissions(file *drive.File) string {
	var permissionEmails string

	for count, permission := range file.Permissions {
		if strings.Contains(permission.Role, "owner") {
			continue
		}
		p := permission.EmailAddress //+ "(" + currentPermission.Role + ")"
		permissionEmails += p
		fmt.Sprint(count)
		if count == len(file.Permissions)-2 {
			break
		}
		permissionEmails += ","

	}
	return permissionEmails
}

func (receiver *GoogleDriveAPI) RemoveUserPermission(fileId string, permission *drive.Permission, execute bool) error {
	if execute == false {
		log.Printf("\t\tWould remove %s from %s *DID NOT EXECUTE*\n", permission.EmailAddress, fileId)
		return nil
	}
	log.Printf("\t\tRemoving %s from %s\n", permission.EmailAddress, fileId)
	err := receiver.Service.Permissions.Delete(fileId, permission.Id).Do()
	if err != nil {
		log.Println(err.Error())
		return err
	}
	return err
}

func (receiver *GoogleDriveAPI) ShareFile(fileId, email, accountType, role string, notify bool) *drive.Permission {
	response, err := receiver.Service.
		Permissions.
		Create(fileId, &drive.Permission{EmailAddress: email, Type: accountType, Role: strings.ToLower(role)}).
		Fields("*").
		SendNotificationEmail(notify).
		Do()

	if err != nil {
		log.Printf("Sharing: %s, to: %s as [%s, %s] FAILED", fileId, email, accountType, role)
		log.Println(err.Error())
		panic(err)
	} else {
		log.Printf("Sharing: %s, to: %s as [%s, %s] SUCCESS", fileId, email, accountType, role)

	}
	return response
}

/*Workers*/
func (receiver *GoogleDriveAPI) CopyFileWorker(fileInformation []string, wg *sync.WaitGroup) {
	receiver.CopyFile(fileInformation[0], fileInformation[1], fileInformation[2])
	wg.Done()
}

func (receiver *GoogleDriveAPI) RemoveUserPermissionWorker(fileID string, permission *drive.Permission, wg *sync.WaitGroup, execute bool) error {
	err := receiver.RemoveUserPermission(fileID, permission, execute)
	wg.Done()
	return err
}

func (receiver GoogleDriveAPI) RemoveUserPermissionByIdWorker(fileID, permissionId string, wg *sync.WaitGroup, execute bool) error {
	var err error
	if execute == true {
		err = receiver.Service.Permissions.Delete(fileID, permissionId).Do()
	} else {
		log.Printf("Would remove [%s] from: %s\n", permissionId, fileID)
		wg.Done()
		return err
	}

	if err != nil {
		log.Println(err.Error())
	} else {
		log.Printf("Removed [%s] from: %s\n", permissionId, fileID)
	}
	wg.Done()
	return err
}

func (receiver GoogleDriveAPI) GetFileBlobByID(fileId string) (*drive.File, []byte) {
	//Get file information
	log.Printf("Downloading %s as a blob from Google Drive...\n", fileId)
	driveFile := receiver.GetFileById(fileId)
	log.Printf("Retreiving file [%s] data from Google Drive...\n", fileId)
	if strings.Contains(driveFile.MimeType, "google") {
		osMimeType, ext := GetOSMimeType(driveFile.MimeType)
		driveFile.OriginalFilename = driveFile.Name + ext
		response, err := receiver.Service.Files.Export(fileId, osMimeType).Download()
		if err != nil {
			log.Println(err.Error())
			panic(err)
		}
		blob, err := ioutil.ReadAll(response.Body)
		if err != nil {
			log.Println(err.Error())
			panic(err)
		}

		log.Printf("Pulled \"%s\" blob from Google Drive...\n", ByteCount(driveFile.Size))
		return driveFile, blob
	}

	response, err := receiver.Service.Files.Get(fileId).Download()
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	blob, err := ioutil.ReadAll(response.Body)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	log.Printf("Pulled \"%s\" blob from Google Drive...\n", ByteCount(driveFile.Size))
	return driveFile, blob
}

func (receiver GoogleDriveAPI) GetInMemoryDriveFileBlob(fileId string) *DriveFile {
	driveFile, fileData := receiver.GetFileBlobByID(fileId)
	localFile := &DriveFile{
		OriginalFileID: fileId,
		FullFileName:   driveFile.Name + path.Ext(driveFile.Name),
		Blob:           fileData,
		DriveInfo:      driveFile,
		FileExtension:  path.Ext(driveFile.Name),
	}
	log.Printf("Downloaded %s to [%s]\n", driveFile.Name)
	return localFile
}

func (df *DriveFile) Save(locationPath string) *DriveFile {
	if df.Blob == nil {
		log.Printf("Cannot save @[%s] because it has no data\n", &df)
		return df
	}
	_, err := os.Stat(locationPath)
	if err != nil {
		if os.IsNotExist(err) {
			if err := os.Mkdir(locationPath, os.ModePerm); err != nil {
				log.Println(err.Error())
				return df
			}
			log.Printf("Created path [%s]\n", locationPath)
		}
	}
	err = os.WriteFile(locationPath+df.FullFileName, df.Blob, os.ModePerm)
	if err != nil {
		if err != nil {
			log.Println(err.Error())
			return df
		}
	}
	fileInfo, err := os.Stat(locationPath + df.FullFileName)
	if err != nil {
		log.Println(err.Error())
		return df
	}
	df.FileInfo = fileInfo
	return df
}

func ByteCount(b int64) string {
	const unit = 1000
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.1f %cB",
		float64(b)/float64(div), "kMGTPE"[exp])
}

func GetOSMimeType(googleWorkspaceMimeType string) (string, string) {
	switch googleWorkspaceMimeType {
	case "application/vnd.google-apps.spreadsheet":
		return "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet", ".xlsx"
	case "application" +
		"/vnd.google-apps.document":
		return "application/vnd.openxmlformats-officedocument.wordprocessingml.document", ".docx"
	case "application/vnd.google-apps.presentation":
		return "application/vnd.openxmlformats-officedocument.presentationml.presentation", ".pptx"
	case "application/vnd.google-apps.script":
		return "text/javascript", ".js"
	case "application/vnd.google-apps.photo":
		return "image/png", ".png"
	case "application/vnd.google-apps.video":
		return "video/mp4", ".mp4"
	case "application/vnd.google-apps.drawing":
		return "image/png", ".png"
	case "application/vnd.google-apps.audio":
		return "audio/mpeg", ".mp3"
	case "application/vnd.google-apps.site":
		return "text/plain", ".txt"
	default:
		return "", ""
	}
}