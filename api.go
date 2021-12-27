package googledrive4go

import (
	"context"
	"fmt"
	"google.golang.org/api/drive/v3"
	"google.golang.org/api/googleapi"
	"google.golang.org/api/option"
	"io"
	"io/ioutil"
	"log"
	"os"
	"strings"
	"sync"
	"time"
)

var ctx = context.Background()

func Initialize(option *option.ClientOption, subject string) *GoogleDrive {
	service, err := drive.NewService(ctx, *option)
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	log.Printf("Initialized GoogleDrive4Go as (%s)\n", subject)
	return &GoogleDrive{Service: service, Subject: subject}
}

type GoogleDrive struct {
	Service *drive.Service
	Subject string
}

/*Files*/
func (receiver *GoogleDrive) GetFileById(fileId string) *drive.File {
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

func (receiver *GoogleDrive) QueryFiles(q string) []*drive.File {
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

func (receiver *GoogleDrive) MoveFile(fileId, parentFolderId string) *drive.File {
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

func (receiver *GoogleDrive) CopyFile(fileId, parentFolderId, fileName string) *drive.File {
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

func (receiver *GoogleDrive) ChangeFileOwner(newOwner, fileId string, doit bool) *drive.Permission {
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

func (receiver *GoogleDrive) ChangeFileOwnerWorker(newOwner, fileId string, doit bool, wg *sync.WaitGroup) {
	receiver.ChangeFileOwner(newOwner, fileId, doit)
	wg.Done()
}

func (receiver *GoogleDrive) UploadFile(absoluteFilePath, parentFolderId string) (*drive.File, error) {
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
func (receiver *GoogleDrive) CopyFolder(sourceFolderId, newSourceFolderName, parentFolderId string) {

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

func (receiver *GoogleDrive) CreateFolder(folderName, parentFolderId string, permissions []*drive.Permission, restricted bool) *drive.File {
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

func (receiver *GoogleDrive) GetNestedFiles(targetFolderId string) []*drive.File {
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
func (receiver *GoogleDrive) GetFilePermissions(file *drive.File) string {
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

func (receiver *GoogleDrive) RemoveUserPermission(fileId string, permission *drive.Permission, execute bool) error {
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

func (receiver *GoogleDrive) ShareFile(fileId, email, accountType, role string) *drive.Permission {
	response, err := receiver.Service.
		Permissions.
		Create(fileId, &drive.Permission{EmailAddress: email, Type: accountType, Role: strings.ToLower(role)}).
		Fields("*").
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
func (receiver *GoogleDrive) CopyFileWorker(fileInformation []string, wg *sync.WaitGroup) {
	receiver.CopyFile(fileInformation[0], fileInformation[1], fileInformation[2])
	wg.Done()
}

func (receiver *GoogleDrive) RemoveUserPermissionWorker(fileID string, permission *drive.Permission, wg *sync.WaitGroup, execute bool) error {
	err := receiver.RemoveUserPermission(fileID, permission, execute)
	wg.Done()
	return err
}

func (receiver GoogleDrive) RemoveUserPermissionByIdWorker(fileID, permissionId string, wg *sync.WaitGroup, execute bool) error {
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

func (receiver GoogleDrive) GetFileDataById(fileId string) (*drive.File, io.ReadCloser) {
	//Get file information
	fileInfo := receiver.GetFileById(fileId)
	log.Printf("Retreiving file [%s] data from Google Drive...\n", fileId)
	if strings.Contains(fileInfo.MimeType, "google") {
		fileInfo.Name = fileInfo.Name + fileInfo.FullFileExtension
		response, err := receiver.Service.Files.Export(fileId, fileInfo.MimeType).Download()
		if err != nil {
			log.Println(err.Error())
			panic(err)
		}
		return fileInfo, response.Body
	}
	response, err := receiver.Service.Files.Get(fileId).Download()
	if err != nil {
		log.Println(err.Error())
		panic(err)
	}

	log.Printf("Retreived [%s] from Google Drive...\n", ByteCount(fileInfo.Size))
	return fileInfo, response.Body
}

func (receiver GoogleDrive) DownloadFileById(fileId, location string) ([]byte, error) {
	log.Printf("Downloading %s from Google Drive...\n", fileId)

	if _, err := os.Stat(location); os.IsNotExist(err) {
		if err := os.Mkdir(location, os.ModePerm); err != nil {
			log.Println(err.Error())
			return nil, err
		}
		log.Printf("Created path [%s]\n", location)
	}
	file, res := receiver.GetFileDataById(fileId)
	fileData, err := ioutil.ReadAll(res)
	if err != nil {
		log.Println(err.Error())
		return nil, err
	}
	os.WriteFile(location+file.Name, fileData, os.ModePerm)
	log.Printf("Downloaded %s to [%s]\n", file.Name, location)
	return os.ReadFile(location + file.Name)
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
